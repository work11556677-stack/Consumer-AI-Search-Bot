#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robust missing-only DOCX→PDF for OneDrive/Windows (no argparse, globals only)

What it does
- Scans ROOT_DIR recursively.
- For each *.docx that lacks a same-name *.pdf in the same folder, convert it.
- Stages only the needed DOCX into a local temp tree, strips Zone.Identifier,
  then uses ONE Word COM session (pywin32) to ExportAsFixedFormat (PDF).
- Moves PDFs back to original locations.

Why it fixes "Open.SaveAs"
- OneDrive/Internet files often have a Zone.Identifier that triggers Protected View.
- Staging + unblocking avoids that; single COM session avoids repeated Open/SaveAs churn.

Prereqs
- Windows + Microsoft Word installed
- Python package: pywin32  (pip install pywin32)
"""

from __future__ import annotations
import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

# =======================
# CONFIG — EDIT THESE
# =======================
ROOT_DIR: Path = Path(r"Docx Retail")
OVERWRITE: bool = False              # True → recreate PDFs even if they already exist
INCLUDE_HIDDEN: bool = False         # False → skip "~$..." and dotfiles
BATCH_CLOSE_DOCS: int = 200          # Close/restart Word after this many docs (defensive)
LOG_EVERY: int = 25                  # Progress print interval

# =======================
# Helpers
# =======================
# --- add near your imports ---
import time
import pythoncom
import win32com.client as win32
from pywintypes import com_error

RPC_E_CALL_REJECTED = -2147418111  # 0x80010001

def _rpc_retry(fn, *args, retries=10, sleep=0.5, **kwargs):
    """
    Call a COM function with retry if Word rejects the call (RPC_E_CALL_REJECTED).
    Pumps Windows messages between retries to let Word process its modal state.
    """
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except com_error as e:
            hr = e.hresult if hasattr(e, "hresult") else (e.args[0] if e.args else None)
            if hr == RPC_E_CALL_REJECTED:
                # Give Word some breathing room
                pythoncom.PumpWaitingMessages()
                time.sleep(sleep)
                continue
            raise
    # final attempt without catching so we see the real error
    return fn(*args, **kwargs)

def is_hidden_or_temp(p: Path) -> bool:
    name = p.name
    return name.startswith("~$") or name.startswith(".")

def find_missing(root: Path, include_hidden: bool, overwrite: bool) -> Dict[Path, List[Path]]:
    need: Dict[Path, List[Path]] = {}
    for dirpath, _, filenames in os.walk(root):
        d = Path(dirpath)
        pick: List[Path] = []
        for fn in filenames:
            p = d / fn
            if p.suffix.lower() != ".docx":
                continue
            if not include_hidden and is_hidden_or_temp(p):
                continue
            pdf = p.with_suffix(".pdf")
            if overwrite or not pdf.exists():
                pick.append(p)
        if pick:
            need[d] = pick
    return need

def remove_zone_identifier(path: Path) -> None:
    """Remove NTFS Zone.Identifier stream to avoid Protected View. Ignore errors."""
    try:
        zi = str(path) + ":Zone.Identifier"
        if os.path.exists(zi):
            os.remove(zi)
    except Exception:
        pass

def ensure_local_copy(src: Path, dst: Path) -> bool:
    """Copy src → dst (creating parent dirs). Return True on success."""
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)  # forces hydration from OneDrive if needed
        # Strip Protected View zone tag on the staged copy
        remove_zone_identifier(dst)
        return True
    except Exception as e:
        print(f"[stage] COPY FAIL: {src} -> {dst} :: {e}")
        return False

def word_open_export_close(app, in_docx: Path, out_pdf: Path) -> bool:
    """
    Open DOCX read-only and export to PDF with robust retries around COM calls.
    Also rescues Protected View by promoting to edit mode if needed.
    """
    wdExportFormatPDF = 17

    # Defensive: ensure output dir exists and no stale file blocks us
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    if out_pdf.exists():
        try:
            out_pdf.unlink()
        except Exception:
            pass

    # 1) Open with retries
    try:
        doc = _rpc_retry(
            app.Documents.Open,
            str(in_docx),
            ReadOnly=True,
            AddToRecentFiles=False,
            ConfirmConversions=False,
            Visible=False,
            OpenAndRepair=True,
        )
    except com_error as e:
        # Try Protected View rescue path
        try:
            pvw = app.ProtectedViewWindows
            if pvw and pvw.Count > 0:
                # Activate last PV window and edit it, then export
                pv = pvw(pvw.Count)
                _rpc_retry(pv.Edit)
                doc = app.ActiveDocument  # now editable
            else:
                raise
        except Exception:
            print(f"[word] FAIL Export (open): {in_docx.name} :: {e}")
            return False

    # 2) Export with retries
    try:
        _rpc_retry(doc.ExportAsFixedFormat, str(out_pdf), wdExportFormatPDF, False)
    except com_error as e:
        print(f"[word] FAIL Export (export): {in_docx.name} :: {e}")
        try:
            _rpc_retry(doc.Close, False)
        except Exception:
            pass
        return False

    # 3) Close with retries
    try:
        _rpc_retry(doc.Close, False)
    except Exception:
        pass

    return out_pdf.exists()
def make_word_app():
    """
    Start a clean Word instance with safer defaults:
    - Separate instance via DispatchEx
    - No UI noise / prompts
    - Macros disabled (AutomationSecurity = 3)
    """
    pythoncom.CoInitialize()  # ensure COM apartment is initialized
    app = win32.DispatchEx("Word.Application")
    app.Visible = False
    try:
        app.DisplayAlerts = 0  # wdAlertsNone
    except Exception:
        pass
    try:
        # 3 = msoAutomationSecurityForceDisable
        app.AutomationSecurity = 3
    except Exception:
        pass
    try:
        # Reduce background interference
        app.Options.BackgroundSave = False
        app.Options.AllowReadingMode = False
        app.Options.ConfirmConversions = False
        app.Options.SavePropertiesPrompt = False
    except Exception:
        pass
    return app

# =======================
# Main
# =======================

def main():
    root = ROOT_DIR.resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Source directory does not exist or is not a directory: {root}")

    tasks = find_missing(root, INCLUDE_HIDDEN, OVERWRITE)
    total = sum(len(v) for v in tasks.values())
    print(f"[scan] Root: {root}")
    print(f"[scan] DOCX needing conversion: {total}")
    if total == 0:
        print("[done] Nothing to do.")
        return

    # Stage to a local temp tree
    import win32com.client  # fail fast if pywin32 missing
    with tempfile.TemporaryDirectory(prefix="docx2pdf_stage_") as tmp:
        stage_root = Path(tmp)

        rels: List[Tuple[Path, Path]] = []  # (staged_docx, original_docx)
        i = 0
        for folder, files in tasks.items():
            for src in files:
                rel = src.relative_to(root)
                staged = stage_root / rel
                if ensure_local_copy(src, staged):
                    rels.append((staged, src))
                i += 1
                if i % LOG_EVERY == 0:
                    print(f"[stage] Copied {i}/{total}")

        # If overwriting, clear destination PDFs now to avoid any move collisions later
        if OVERWRITE:
            cleared = 0
            for _, orig in rels:
                outpdf = orig.with_suffix(".pdf")
                if outpdf.exists():
                    try:
                        outpdf.unlink()
                        cleared += 1
                    except Exception:
                        pass
            if cleared:
                print(f"[clean] Removed {cleared} existing PDFs due to OVERWRITE=True")

        # Start Word
        print("[engine] Word COM (single session)")
        app = make_word_app()

        ok = 0
        fail = 0
        opened = 0

        try:
            for idx, (staged_docx, orig_docx) in enumerate(rels, 1):
                staged_pdf = staged_docx.with_suffix(".pdf")
                # Ensure parent exists
                staged_pdf.parent.mkdir(parents=True, exist_ok=True)

                # Export
                if word_open_export_close(app, staged_docx, staged_pdf):
                    # Move PDF back to original folder
                    dest_pdf = orig_docx.with_suffix(".pdf")
                    dest_pdf.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        # If somehow exists and not overwriting, skip
                        if dest_pdf.exists() and not OVERWRITE:
                            print(f"[skip] Exists: {dest_pdf}")
                        else:
                            shutil.move(str(staged_pdf), str(dest_pdf))
                        ok += 1
                    except Exception as e:
                        print(f"[move] FAIL {staged_pdf} -> {dest_pdf} :: {e}")
                        fail += 1
                else:
                    fail += 1

                opened += 1
                if opened % LOG_EVERY == 0:
                    print(f"[prog] {opened}/{len(rels)} converted (ok={ok}, fail={fail})")

                # Defensive recycle of Word after big batches
                if opened % BATCH_CLOSE_DOCS == 0:
                    try:
                        app.Quit(False)
                    except Exception:
                        pass
                    app = make_word_app()

        finally:
            try:
                app.Quit(False)
            except Exception:
                pass

        print(f"[summary] Converted: {ok} | Failed: {fail}")

if __name__ == "__main__":
    main()
