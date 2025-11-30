# -*- coding: utf-8 -*-

"""
V4D2 — DOCX-XML DEBUG marker mapping (no PDF scraping), robust embeddings
20251020

ingest_docx_tree.py — DOCX-first, tree-aware ingestor for pdfint.db

- Parses DOCX into a nested tree (sections, paragraphs, tables, figures)
- Uses DEBUG marker pages embedded in the DOCX flow:
    "------ DEBUG Page X ------"
  Each such marker page labels the *next* non-blank content as logical page X
- Skips marker pages from indexing; assigns meta.pdf_page=X to content until next marker
- Saves inline images to OUT_DIR
- Embeds chunks (OpenAI) with fail-open handling
- Builds FTS
"""

import os, re, json, argparse, sqlite3, datetime, glob, uuid
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from io import BytesIO

import numpy as np
from PIL import Image

# ---- DOCX deps ----
import docx
from docx import Document
from docx.text.paragraph import Paragraph as _Paragraph
from docx.table import Table as _Table
from docx.oxml.table import CT_Tbl
from docx.oxml.text.paragraph import CT_P
from docx.oxml.ns import qn

# ---- OpenAI (embedding) ----
from openai import OpenAI

# ---------- Config (env overridable) ----------
DEFAULT_DB = os.getenv("MAIN_DB_PATH",
    r"pdfint.db"
)
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")

# Read from the DEBUG-inserted copies (these contain the blank page marker pages):
SOURCE_ROOT = os.getenv("SOURCE_ROOT",
    r"Docx Retail copy"
)
# Store URIs pointing to originals (without DEBUG pages):
ORIGINAL_ROOT = os.getenv("ORIGINAL_ROOT",
    r"Docx Retail"
)

OUT_DIR   = os.getenv("OUT_DIR",
    r"out_docx_tree"
)

client = OpenAI()  # requires OPENAI_API_KEY

# ---------- DB schema ----------
MAIN_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS ref_company (
  company_id   INTEGER PRIMARY KEY,
  legal_name   TEXT NOT NULL,
  ticker       TEXT
);

CREATE TABLE IF NOT EXISTS document (
  document_id  INTEGER PRIMARY KEY,
  title        TEXT,
  published_at TEXT,
  file_uri     TEXT,
  mime_type    TEXT,
  meta         TEXT
);

CREATE TABLE IF NOT EXISTS document_company (
  document_id  INTEGER NOT NULL REFERENCES document(document_id) ON DELETE CASCADE,
  company_id   INTEGER NOT NULL REFERENCES ref_company(company_id) ON DELETE CASCADE,
  PRIMARY KEY (document_id, company_id)
);

CREATE TABLE IF NOT EXISTS chunk (
  chunk_id     INTEGER PRIMARY KEY,
  document_id  INTEGER NOT NULL REFERENCES document(document_id) ON DELETE CASCADE,
  text         TEXT,
  section      TEXT,
  chunk_index  INTEGER,
  page_start   INTEGER,
  page_end     INTEGER,
  meta         TEXT,
  embedding    BLOB
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
  text, section, title, doc_meta,
  tokenize='unicode61',
  content='chunk',
  content_rowid='chunk_id'
);
"""

# ---------- Helpers ----------
def _norm_text(s: str) -> str:
    s = (s or "")
    s = s.replace("\u00ad", "")
    s = re.sub(r"-\s+\n", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def connect(db_path: str) -> sqlite3.Connection:
    db_exists = os.path.exists(db_path)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    if not db_exists:
        conn.executescript(MAIN_SCHEMA_SQL); conn.commit()
    return conn

def ensure_company_links(conn: sqlite3.Connection, document_id: int, text_for_detect: str):
    tickers = set(re.findall(r"\b[A-Z]{3,4}\b", text_for_detect or ""))
    if not tickers:
        return
    if not conn.execute("SELECT COUNT(*) AS c FROM ref_company").fetchone()["c"]:
        return
    for t in tickers:
        row = conn.execute("SELECT company_id FROM ref_company WHERE UPPER(ticker)=?", (t.upper(),)).fetchone()
        if row:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO document_company(document_id, company_id) VALUES (?,?)",
                    (document_id, row["company_id"])
                )
            except Exception:
                pass

def embed_texts(texts: List[str]) -> List[Optional[np.ndarray]]:
    if not texts:
        return []
    try:
        resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
        out: List[Optional[np.ndarray]] = []
        for d in resp.data:
            v = np.asarray(d.embedding, dtype=np.float32)
            v = v / (np.linalg.norm(v) + 1e-9)
            out.append(v.astype(np.float32))
        return out
    except Exception as e:
        print(f"[embed] WARNING: embedding failed; continuing without vectors: {e}")
        return [None for _ in texts]

def title_from_filename(path: str) -> str:
    base = os.path.basename(path)
    title = re.sub(r"\.(docx)$", "", base, flags=re.I)
    title = re.sub(r"[_\-]+", " ", title).strip()
    return title or base

def remap_to_original_root(full_path: str) -> str:
    try:
        rel = os.path.relpath(full_path, start=SOURCE_ROOT)
        return os.path.join(ORIGINAL_ROOT, rel)
    except Exception:
        return full_path.replace("Docx Retail copy", "Docx Retail", 1)

def to_db_uri(path: str, root: str) -> str:
    try:
        rel = os.path.relpath(path, start=root)
    except Exception:
        rel = path
    return rel.replace("\\", "/")

def discover_files(root: str, pattern: str) -> List[str]:
    root = os.path.abspath(root)
    pats = [pattern] if pattern else ["**/*.docx"]
    out: List[str] = []
    for p in pats:
        out.extend(glob.glob(os.path.join(root, p), recursive=True))
    seen, uniq = set(), []
    for f in out:
        if f not in seen:
            uniq.append(f); seen.add(f)
    return uniq

# ---------- Date extraction (YYMMDD at end of filename) ----------
def extract_publish_date(path: str) -> Optional[str]:
    name = os.path.basename(path)
    stem, _ = os.path.splitext(name)
    m = re.search(r'(\d{6})$', stem)
    if m:
        yymmdd = m.group(1)
        yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
        year = 2000 + yy
        try:
            dt = datetime.date(year, mm, dd)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        ts = datetime.datetime.fromtimestamp(os.path.getmtime(path))
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None

# ---------- DOCX block iteration ----------
CAPTION_STYLE_NAMES = {"caption", "figure", "figure caption"}
RE_DEBUG_LINE = re.compile(r"^\s*-*\s*DEBUG\s+Page\s+(\d+)\s*-*\s*$", re.IGNORECASE)

def is_caption_paragraph_text_style(style_name: Optional[str], text: str) -> bool:
    style = (style_name or "").strip().lower() if style_name else ""
    if style in CAPTION_STYLE_NAMES:
        return True
    return bool(re.match(r'^(fig(?:ure)?|table)\b', (text or "").strip(), flags=re.IGNORECASE))

def _iter_body_children(parent):
    if hasattr(parent, "_element") and hasattr(parent._element, "body") and parent._element.body is not None:
        return parent._element.body
    if hasattr(parent, "_tc"):
        return parent._tc
    return getattr(parent, "_element", parent)

def iter_block_items(parent):
    body = _iter_body_children(parent)
    for child in body.iterchildren():
        if isinstance(child, CT_P) or child.tag == qn("w:p"):
            yield _Paragraph(child, parent)
        elif isinstance(child, CT_Tbl) or child.tag == qn("w:tbl"):
            yield _Table(child, parent)

# ---------- Images ----------
def save_image_part(image_part, media_dir: Path, preferred_ext: Optional[str]=None) -> Dict[str, Any]:
    image_bytes = image_part.blob
    content_type = image_part.content_type
    ext_map = {
        "image/png": ".png", "image/jpeg": ".jpg", "image/jpg": ".jpg",
        "image/gif": ".gif", "image/tiff": ".tif", "image/bmp": ".bmp",
        "image/svg+xml": ".svg", "image/webp": ".webp",
        "image/x-emf": ".emf", "image/x-wmf": ".wmf", "image/x-wmz": ".wmz",
        "image/x-emz": ".emz",
    }
    ext = preferred_ext or ext_map.get(content_type, ".bin")
    media_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{uuid.uuid4().hex}{ext}"
    fpath = media_dir / fname
    with open(fpath, "wb") as f:
        f.write(image_bytes)
    width_px = height_px = None
    try:
        im = Image.open(BytesIO(image_bytes))
        width_px, height_px = im.size
    except Exception:
        pass
    meta = {
        "type": "image",
        "file": fpath.as_posix(),
        "content_type": content_type,
        "width_px": width_px,
        "height_px": height_px,
    }
    return meta

def extract_images_from_runs(paragraph, media_dir: Path) -> List[Dict[str, Any]]:
    images: List[Dict[str, Any]] = []
    for run in paragraph.runs:
        r_el = run._element
        blips = r_el.xpath(".//a:blip")
        for blip in blips:
            rId = blip.get(qn("r:embed"))
            if not rId:
                continue
            image_part = paragraph.part.related_parts.get(rId)
            if image_part is None:
                continue
            meta = save_image_part(image_part, media_dir)
            try:
                extent = r_el.xpath(".//wp:extent")
                if extent:
                    cx = extent[0].get("cx"); cy = extent[0].get("cy")
                    if cx and cy:
                        meta["drawn_width_px"]  = int(round(int(cx) / 9525))
                        meta["drawn_height_px"] = int(round(int(cy) / 9525))
            except Exception:
                pass
            images.append(meta)
    return images

def paragraph_to_node(p, media_dir: Path) -> Dict[str, Any]:
    return {
        "type": "paragraph",
        "style": p.style.name if p.style else None,
        "text": p.text or "",
        "inline_images": extract_images_from_runs(p, media_dir),
    }

def cell_block_items(cell, media_dir: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in iter_block_items(cell):
        if item.__class__.__name__ == "Paragraph":
            out.append(paragraph_to_node(item, media_dir))
        else:
            out.append(table_to_node(item, media_dir))
    return out

def make_figure_node(caption_text: str, image_nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"type": "figure", "caption": (caption_text or "").strip(), "images": image_nodes}

def table_to_node(tbl, media_dir: Path) -> Dict[str, Any]:
    try:
        n_rows = len(tbl.rows); n_cols = len(tbl.columns)
    except Exception:
        n_rows = len(getattr(tbl._tbl, "tr_lst", [])); n_cols = 1
    if n_rows >= 2 and n_cols == 1:
        row0_blocks = cell_block_items(tbl.rows[0].cells[0], media_dir)
        row1_blocks = cell_block_items(tbl.rows[1].cells[0], media_dir)
        cap_text = None
        if row0_blocks and row0_blocks[0].get("type") == "paragraph":
            p0 = row0_blocks[0]
            if is_caption_paragraph_text_style(p0.get("style"), p0.get("text", "")):
                cap_text = p0.get("text", "")
        image_nodes: List[Dict[str, Any]] = []
        for b in row1_blocks:
            if b.get("type") == "paragraph" and b.get("inline_images"):
                image_nodes.extend(b["inline_images"])
        if cap_text and image_nodes:
            return make_figure_node(cap_text, image_nodes)
    rows_json: List[List[Dict[str, Any]]] = []
    for row in tbl.rows:
        row_json: List[Dict[str, Any]] = []
        for cell in row.cells:
            row_json.append({"type": "cell", "blocks": cell_block_items(cell, media_dir)})
        rows_json.append(row_json)
    return {"type": "table", "rows": rows_json}

def heading_level_from_style(style_name: Optional[str]) -> Optional[int]:
    if not style_name:
        return None
    m = re.match(r"Heading\s+(\d+)", style_name, re.IGNORECASE)
    if m: return int(m.group(1))
    if style_name and style_name.lower() == "title": return 1
    if style_name and style_name.lower() == "subtitle": return 2
    return None

def coalesce_figures_in_blocks(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    i = 0
    while i < len(blocks):
        cur = blocks[i]
        nxt = blocks[i+1] if i+1 < len(blocks) else None
        if (cur.get("type") == "paragraph"
            and is_caption_paragraph_text_style(cur.get("style"), cur.get("text",""))
            and nxt and nxt.get("type") == "paragraph" and nxt.get("inline_images")):
            out.append(make_figure_node(cur.get("text",""), nxt.get("inline_images")))
            i += 2; continue
        if cur.get("type") == "section":
            cur["children"] = coalesce_figures_in_blocks(cur.get("children", []))
        out.append(cur); i += 1
    return out

def build_tree(document: Document, media_dir: Path) -> Dict[str, Any]:
    """Tree is useful for UI + media; page labels are handled in the linear iterator below."""
    root: Dict[str, Any] = {"type": "document", "children": []}
    section_stack: List[Dict[str, Any]] = [root]
    def push_section(title: str, level: int):
        node = {"type": "section", "title": title, "level": level, "children": []}
        while len(section_stack) > 1 and section_stack[-1].get("level", 0) >= level:
            section_stack.pop()
        section_stack[-1]["children"].append(node)
        section_stack.append(node)
    for blk in iter_block_items(document):
        if blk.__class__.__name__ == "Paragraph":
            level = heading_level_from_style(blk.style.name if blk.style else None)
            if level:
                push_section(blk.text.strip(), level)
            else:
                section_stack[-1]["children"].append(paragraph_to_node(blk, media_dir))
        else:
            section_stack[-1]["children"].append(table_to_node(blk, media_dir))
    root["children"] = coalesce_figures_in_blocks(root["children"])
    return root

# ---------- NEW: Linear iterator with DEBUG page labels from DOCX ----------
def iter_text_chunks_with_debug_labels(document: Document, media_dir: Path) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Single-pass over the DOCX flow. Whenever we see a paragraph that matches
    '------ DEBUG Page X ------' (and nothing else), we set current_label=X and skip that page.
    All following content gets meta.pdf_page=X until the next marker.
    Section_path is maintained from Heading N styles.
    """
    out: List[Tuple[str, Dict[str, Any]]] = []
    section_path: List[str] = []
    current_label: Optional[int] = None

    def push(text: str, kind: str):
        t = (text or "").strip()
        if not t:
            return
        meta = {"kind": kind, "section_path": " > ".join([p for p in section_path if p]) or None}
        if current_label is not None:
            meta["pdf_page"] = int(current_label)
        out.append((t, meta))

    for blk in iter_block_items(document):
        if blk.__class__.__name__ == "Paragraph":
            raw = (blk.text or "").strip()
            # Is this a title/heading?
            level = heading_level_from_style(blk.style.name if blk.style else None)

            # Check DEBUG marker (exact page marker line)
            # We also tolerate stray spaces/dashes
            if not level:
                norm = re.sub(r"\s+", " ", raw)
                m = RE_DEBUG_LINE.match(norm)
                if m:
                    try:
                        current_label = int(m.group(1))
                        print(f"[DOCX marker] set current pdf_page = {current_label}")
                    except Exception:
                        pass
                    # Skip indexing this marker paragraph (it's the blank page)
                    continue

            if level:
                # update section path
                title = raw
                while len(section_path) >= level:
                    if section_path: section_path.pop()
                    else: break
                section_path.append(title)
                continue  # headings are not indexed as text chunks
            else:
                # normal paragraph → include (with label if set)
                push(raw, "paragraph")

        else:
            # Table → extract cell texts with same current_label
            tbl = blk
            try:
                for row in tbl.rows:
                    for cell in row.cells:
                        parts: List[str] = []
                        for item in iter_block_items(cell):
                            if item.__class__.__name__ == "Paragraph":
                                txt = (item.text or "").strip()
                                if txt:
                                    parts.append(txt)
                        if parts:
                            push(" ".join(parts), "table_cell")
            except Exception:
                pass

    return out

# ---------- Ingest one DOCX ----------
def ingest_one_docx(conn: sqlite3.Connection, full_path: str, out_root: Path):
    title = title_from_filename(full_path)
    meta_base = {"source": "docx_ingest", "absolute_path": full_path}

    # Build tree + save media (for viewer/debug)
    doc = Document(full_path)
    media_dir = out_root / (Path(full_path).stem + "_media")
    tree = build_tree(doc, media_dir)

    # Published date (YYMMDD at end)
    pub = extract_publish_date(full_path)

    # Save tree JSON
    out_root.mkdir(parents=True, exist_ok=True)
    tree_path = out_root / (Path(full_path).stem + "_tree.json")
    with open(tree_path, "w", encoding="utf-8") as f:
        json.dump(tree, f, ensure_ascii=False, indent=2)

    # Insert document with URI remapped to originals
    mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    original_path = remap_to_original_root(full_path)
    original_uri  = to_db_uri(original_path, ORIGINAL_ROOT)
    meta = {**meta_base, "tree_json_path": tree_path.as_posix(), "page_source":"docx_debug_marker_pages"}
    doc_id = conn.execute(
        "INSERT INTO document(title, published_at, file_uri, mime_type, meta) VALUES (?,?,?,?,?)",
        (title, pub, original_uri, mime, json.dumps(meta, ensure_ascii=False))
    ).lastrowid
    print(f"[ingest] doc_id={doc_id} -> {title} (pub={pub})")

    # Build chunks directly from DOCX flow with DEBUG labels
    chunks = iter_text_chunks_with_debug_labels(doc, media_dir)
    texts  = [re.sub(r"\s+", " ", t[0]).strip()[:6000] for t in chunks]
    vecs   = embed_texts(texts) if texts else []

    # Link tickers
    ensure_company_links(conn, doc_id, " ".join(texts)[:100000])

    # Insert chunks (+ FTS) with real pdf_page from the DOCX markers
    for i, ((text, meta_small), vec) in enumerate(zip(chunks, vecs)):
        pdf_page = meta_small.get("pdf_page")
        meta_small2 = dict(meta_small)

        conn.execute(
            "INSERT INTO chunk(document_id, text, section, chunk_index, page_start, page_end, meta, embedding) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                int(doc_id),
                text,
                meta_small2.get("section_path"),
                i,
                (int(pdf_page) if isinstance(pdf_page, int) else None),
                (int(pdf_page) if isinstance(pdf_page, int) else None),
                json.dumps(meta_small2, ensure_ascii=False),
                (vec.tobytes() if isinstance(vec, np.ndarray) else None),
            )
        )
        rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT INTO chunk_fts(rowid, text) VALUES (?, ?)", (rid, text))

# ---------- Migration helper ----------
def rebuild_chunk_fts_from_existing(conn: sqlite3.Connection):
    conn.execute("DELETE FROM chunk_fts;")
    conn.execute("""
        INSERT INTO chunk_fts(rowid, text, section, title, doc_meta)
        SELECT c.chunk_id,
               COALESCE(c.text,''),
               COALESCE(c.section,''),
               COALESCE(d.title,''),
               COALESCE(json_extract(d.meta,'$.subtitle'),'')
        FROM chunk c
        JOIN document d ON d.document_id = c.document_id;
    """)
    conn.commit()
    print("[migrate] chunk_fts rebuilt from existing rows.")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Ingest DOCX using DOCX-embedded DEBUG pages to map real page numbers")
    ap.add_argument("--root", default=SOURCE_ROOT, help="DOCX root (debug copies)")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite path for main DB")
    ap.add_argument("--glob", default="", help="Glob (e.g. '**/*.docx'); empty = all DOCX")
    ap.add_argument("--out", default=OUT_DIR, help="Output dir for trees/media")
    ap.add_argument("--rebuild-fts", action="store_true", help="Only rebuild chunk_fts from existing rows")
    args = ap.parse_args()

    out_root = Path(args.out)
    conn = connect(args.db)
    try:
        if args.rebuild_fts:
            rebuild_chunk_fts_from_existing(conn)
            return
        files = discover_files(args.root, args.glob)
        if not files:
            print("[ingest] No DOCX files found.")
            return
        for f in files:
            try:
                ingest_one_docx(conn, f, out_root)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"[ingest] ERROR {f}: {e}")
    finally:
        conn.close()
    print("[ingest] done.")

if __name__ == "__main__":
    import datetime
    print("Starting ..... ")
    main()




















