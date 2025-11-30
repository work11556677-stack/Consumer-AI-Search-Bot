#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print company mention counts (name/ticker/alias) for a single document.

Usage examples:
  # by document_id
  python show_company_counts_for_doc.py --doc-id 123

  # by a title fragment (case-insensitive LIKE)
  python show_company_counts_for_doc.py --title-like "coles outlook"

  # use a specific DB
  DB_PATH="C:/path/to/pdfint.db" python show_company_counts_for_doc.py --doc-id 123
"""

import os
import sys
import sqlite3
import argparse

DEFAULT_DB = r"C:\Users\HarryKember\OneDrive - MST Financial\Desktop\20251029 AI Reports\V6\Backend\pdfint.db"
DB_PATH =  DEFAULT_DB

def fetchall(conn, sql, params=()):
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows

def fetchone(conn, sql, params=()):
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    return row

def column_exists(conn, table, col):
    try:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
        return col in cols
    except Exception:
        return False

def resolve_doc_id(conn, doc_id, title_like):
    if doc_id is not None:
        exists = fetchone(conn, "SELECT 1 FROM document WHERE document_id=?", (doc_id,))
        if not exists:
            print(f"[warn] document_id {doc_id} not found in document table.")
        return doc_id
    if title_like:
        if column_exists(conn, "document", "title"):
            row = fetchone(conn, "SELECT document_id FROM document WHERE title LIKE ? COLLATE NOCASE LIMIT 1",
                           (f"%{title_like}%",))
            if row:
                return row[0]
            print(f"[warn] No document with title like '{title_like}'.")
        else:
            print("[warn] document.title column not found; cannot search by title.")
    # fallback: pick the most recently scanned doc in company_term_count
    if column_exists(conn, "company_term_count", "last_scanned_at"):
        row = fetchone(conn, """
            SELECT document_id
            FROM company_term_count
            ORDER BY last_scanned_at DESC
            LIMIT 1
        """)
        if row:
            print(f"[info] Using most recently scanned document_id: {row[0]}")
            return row[0]
    # last fallback: any doc that has counts
    row = fetchone(conn, "SELECT document_id FROM company_term_count LIMIT 1")
    if row:
        print(f"[info] Using first available document_id in company_term_count: {row[0]}")
        return row[0]
    print("[error] No rows in company_term_count. Run your rebuild script first.")
    return None

def print_doc_meta(conn, doc_id):
    # Print whatever metadata exists
    fields = []
    for f in ("title", "published_at", "source_url", "source_path", "db_label"):
        if column_exists(conn, "document", f):
            fields.append(f)
    if not fields:
        print("[meta] (no metadata columns available to show)")
        return
    cols = ", ".join(fields)
    row = fetchone(conn, f"SELECT {cols} FROM document WHERE document_id=?", (doc_id,))
    if not row:
        print("[meta] Document metadata not found.")
        return
    print("[meta] Document:")
    for i, f in enumerate(fields):
        print(f"  {f}: {row[i]}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", type=int, default=None, help="document_id to inspect")
    ap.add_argument("--title-like", type=str, default=None, help="case-insensitive title substring to resolve the doc")
    args = ap.parse_args()

    print(f"[open] DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # sanity checks
    for t in ("document", "company_term_count", "ref_company"):
        r = fetchone(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
        if not r:
            print(f"[error] table '{t}' not found. Aborting.")
            conn.close()
            sys.exit(1)

    did = resolve_doc_id(conn, args.doc_id, args.title_like)
    if did is None:
        conn.close()
        sys.exit(2)

    print_doc_meta(conn, did)

    # pull counts
    cols = ["name_hits", "ticker_hits", "alias_hits", "total_hits", "last_scanned_at"]
    # alias_hits may not exist on very old DBs
    have_alias = column_exists(conn, "company_term_count", "alias_hits")
    select_cols = "name_hits, ticker_hits, total_hits, last_scanned_at" + (", alias_hits" if have_alias else "")
    rows = fetchall(conn, f"""
        SELECT c.company_id, r.legal_name, r.ticker, {select_cols}
        FROM company_term_count c
        JOIN ref_company r ON r.company_id = c.company_id
        WHERE c.document_id = ?
        ORDER BY c.total_hits DESC, c.name_hits DESC, c.ticker_hits DESC
        LIMIT 50
    """, (did,))

    if not rows:
        print(f"[info] No company_term_count rows for document_id {did}.")
        conn.close()
        return

    print("\n[counts] Top matches for document_id", did)
    header = ["ticker", "company", "name_hits", "ticker_hits"]
    if have_alias: header.append("alias_hits")
    header += ["total_hits", "last_scanned_at"]
    print(" | ".join(header))
    print("-" * 80)

    for r in rows:
        line = [
            str(r["ticker"] or ""),
            str(r["legal_name"] or ""),
            str(r["name_hits"]),
            str(r["ticker_hits"]),
        ]
        if have_alias:
            line.append(str(r["alias_hits"]))
        line += [str(r["total_hits"]), str(r["last_scanned_at"])]
        print(" | ".join(line))

    conn.close()

if __name__ == "__main__":
    main()
