#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
One-shot setup + resilient migration:
- Ensures UNIQUE/PK constraints exist (creates UNIQUE indexes if missing)
- De-dupes offending rows before creating UNIQUE indexes
- Seeds ref_company (ticker->name) and ref_company_alias (slang)
- Rebuilds company_term_count with name/ticker/alias hits
"""

import os, re, sys, datetime, sqlite3
from typing import Dict, List, Tuple, Optional

DEFAULT_DB = r"C:\Users\HarryKember\OneDrive - MST Financial\Desktop\20251029 AI Reports\V6\Backend\pdfint.db"
DB_PATH =  DEFAULT_DB


ASX_COMPANIES: Dict[str, str] = {
    "ADH": "Adairs Limited",
    "ALD": "Ampol Limited",
    "AX1": "Accent Group Limited",
    "BAP": "Bapcor Limited",
    "BRG": "Breville Group Limited",
    "CCX": "City Chic Collective Limited",
    "COL": "Coles Group Limited",
    "EDV": "Endeavour Group Limited",
    "DSK": "Dusk Group Limited",
    "HVN": "Harvey Norman Holdings Limited",
    "JBH": "JB Hi-Fi Limited",
    "LOV": "Lovisa Holdings Limited",
    "MTS": "Metcash Limited",
    "MYR": "Myer Holdings Limited",
    "NCK": "Nick Scali Limited",
    "PMV": "Premier Investments Limited",
    "SIG": "Sigma Healthcare Limited",
    "SUL": "Super Retail Group Limited",
    "TPW": "Temple & Webster Group Ltd",
    "VEA": "Viva Energy Group Limited",
    "WES": "Wesfarmers Limited",
    "WOW": "Woolworths Group Limited",
}

ALIASES: Dict[str, List[str]] = {
    "ALD": ["Ampol"],
    "ADH": ["Adairs"],
    "AX1": ["Accent Group", "Accent"],
    "BAP": ["Bapcor"],
    "BRG": ["Breville"],
    "CCX": ["City Chic", "CityChic"],
    "COL": ["Coles"],
    "EDV": ["Endeavour", "Endeavour Group", "Dan Murphy's", "BWS"],
    "DSK": ["Dusk"],
    "HVN": ["Harvey Norman", "HarveyNorman"],
    "JBH": ["JB Hi-Fi", "JBHIFI", "JB HiFi", "JBHiFi", "JB Hifi", "JB"],
    "LOV": ["Lovisa"],
    "MTS": ["Metcash", "IGA"],
    "MYR": ["Myer"],
    "NCK": ["Nick Scali", "NickScali"],
    "PMV": ["Premier Investments", "Premier"],
    "SIG": ["Sigma"],
    "SUL": ["Super Retail", "SuperRetail", "SRG"],
    "TPW": ["Temple & Webster", "Temple&Webster", "Temple and Webster", "T+W"],
    "VEA": ["Viva", "Viva Energy", "VivaEnergy"],
    "WES": ["Wesfarmers", "Wes"],
    "WOW": ["Woolworths", "Woolies", "Woolworths Group"],
}

WORD_BOUNDARY = r"(?<![A-Za-z0-9]){term}(?![A-Za-z0-9])"

# ----------------- small utils -----------------

def safe_regex_count(pattern: Optional[str], text: str) -> int:
    if not pattern or not text: return 0
    try: return len(re.findall(pattern, text, flags=re.I))
    except re.error: return 0

def fetchone(conn, sql, params=()):
    cur = conn.execute(sql, params); r = cur.fetchone(); cur.close(); return r

def fetchall(conn, sql, params=()):
    cur = conn.execute(sql, params); rows = cur.fetchall(); cur.close(); return rows

def table_exists(conn, name: str) -> bool:
    return bool(fetchone(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)))

def index_exists(conn, name: str) -> bool:
    return bool(fetchone(conn, "SELECT name FROM sqlite_master WHERE type='index' AND name=?", (name,)))

# ----------------- migrations (constraints & dedupe) -----------------

def ensure_ref_company(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref_company (
            company_id  INTEGER PRIMARY KEY,
            legal_name  TEXT NOT NULL,
            ticker      TEXT
        );
    """)
    # Deduplicate by ticker before creating unique index
    if table_exists(conn, "ref_company"):
        # Delete duplicate tickers keeping the smallest rowid (or any deterministic choice)
        conn.execute("""
            DELETE FROM ref_company
            WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ref_company
                GROUP BY UPPER(COALESCE(ticker,''))
            )
        """)
        # Ensure UNIQUE index on ticker (so ON CONFLICT(ticker) works even if table was older)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ref_company_ticker ON ref_company(UPPER(ticker))")

def ensure_ref_company_alias(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref_company_alias (
            company_id INTEGER NOT NULL,
            alias      TEXT NOT NULL
        );
    """)
    # Deduplicate pairs
    conn.execute("""
        DELETE FROM ref_company_alias
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM ref_company_alias
            GROUP BY company_id, UPPER(alias)
        )
    """)
    # Ensure unique pair + helpful indexes
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_alias_pair ON ref_company_alias(company_id, UPPER(alias))")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alias_company ON ref_company_alias(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alias_text    ON ref_company_alias(UPPER(alias))")

def ensure_company_term_count(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_term_count (
            document_id     INTEGER NOT NULL,
            company_id      INTEGER NOT NULL,
            name_hits       INTEGER NOT NULL,
            ticker_hits     INTEGER NOT NULL,
            total_hits      INTEGER NOT NULL,
            last_scanned_at TEXT    NOT NULL
        );
    """)
    # Add alias_hits if missing
    cols = [r[1] for r in conn.execute("PRAGMA table_info(company_term_count)")]
    if "alias_hits" not in cols:
        conn.execute("ALTER TABLE company_term_count ADD COLUMN alias_hits INTEGER NOT NULL DEFAULT 0;")
    # Deduplicate before creating unique index
    conn.execute("""
        DELETE FROM company_term_count
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM company_term_count
            GROUP BY document_id, company_id
        )
    """)
    # Ensure unique constraint (PK or unique index) on (document_id, company_id)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ctc_doc_company ON company_term_count(document_id, company_id)")
    # Helpful lookup indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ctc_company  ON company_term_count(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ctc_document ON company_term_count(document_id)")

# ----------------- seeding -----------------

def seed_ref_company(conn: sqlite3.Connection):
    ensure_ref_company(conn)
    for tkr, name in ASX_COMPANIES.items():
        conn.execute("""
            INSERT INTO ref_company(legal_name, ticker)
            VALUES(?, ?)
            ON CONFLICT(UPPER(ticker)) DO UPDATE SET legal_name=excluded.legal_name
        """, (name, tkr))
    conn.commit()
    n = fetchone(conn, "SELECT COUNT(*) FROM ref_company")[0]
    print(f"[seed] ref_company rows now: {n}")

def seed_aliases(conn: sqlite3.Connection):
    ensure_ref_company_alias(conn)
    for tkr, alias_list in ALIASES.items():
        row = fetchone(conn, "SELECT company_id FROM ref_company WHERE UPPER(ticker)=UPPER(?)", (tkr,))
        if not row:
            print(f"[alias] WARN: ticker {tkr} not found; skipping its aliases")
            continue
        cid = row[0]
        for a in alias_list:
            conn.execute("""
                INSERT INTO ref_company_alias(company_id, alias)
                VALUES(?, ?)
                ON CONFLICT(company_id, UPPER(alias)) DO NOTHING
            """, (cid, a))
    conn.commit()
    n = fetchone(conn, "SELECT COUNT(*) FROM ref_company_alias")[0]
    print(f"[alias] ref_company_alias rows now: {n}")

# ----------------- counting -----------------

def summarize_counts(conn: sqlite3.Connection):
    def c(name):
        try: return int(fetchone(conn, f"SELECT COUNT(*) AS c FROM {name}")["c"])
        except Exception: return None
    return {
        "document": c("document"),
        "chunk": c("chunk"),
        "ref_company": c("ref_company"),
        "ref_company_alias": c("ref_company_alias"),
        "document_company": c("document_company"),
    }

def upsert_counts(conn: sqlite3.Connection, rows: List[Tuple]) -> int:
    if not rows: return 0
    # This ON CONFLICT now matches ux_ctc_doc_company unique index
    conn.executemany("""
        INSERT INTO company_term_count(document_id, company_id, name_hits, ticker_hits, alias_hits, total_hits, last_scanned_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(document_id, company_id) DO UPDATE SET
          name_hits       = excluded.name_hits,
          ticker_hits     = excluded.ticker_hits,
          alias_hits      = excluded.alias_hits,
          total_hits      = excluded.total_hits,
          last_scanned_at = excluded.last_scanned_at
    """, rows)
    return len(rows)

def rebuild_counts(conn: sqlite3.Connection):
    counts = summarize_counts(conn)
    print(f"[setup] Row counts: {counts}")
    if not counts.get("document"):
        print("[setup] No documents found. Exiting."); return

    # Prefetch docs & chunks
    doc_ids = [r["document_id"] for r in fetchall(conn, "SELECT document_id FROM document")]
    chunks_by_doc = {}
    BATCH = 1000
    for i in range(0, len(doc_ids), BATCH):
        part = doc_ids[i:i+BATCH]
        q = "SELECT document_id, text FROM chunk WHERE document_id IN ({})".format(",".join("?"*len(part)))
        for row in fetchall(conn, q, tuple(part)):
            chunks_by_doc.setdefault(row["document_id"], []).append(row["text"] or "")

    # Prefetch companies & aliases
    companies = fetchall(conn, "SELECT company_id, legal_name, COALESCE(ticker,'') AS ticker FROM ref_company")
    alias_map = {}
    if counts.get("ref_company_alias"):
        for row in fetchall(conn, "SELECT company_id, alias FROM ref_company_alias"):
            alias_map.setdefault(row["company_id"], []).append((row["alias"] or "").strip())

    now = datetime.datetime.utcnow().isoformat()

    # FAST PATH via document_company
    if counts.get("document_company"):
        print("[ctc] Using document_company links.")
        links = fetchall(conn, """
            SELECT dc.document_id, dc.company_id, rc.legal_name AS name, COALESCE(rc.ticker,'') AS ticker
            FROM document_company dc
            JOIN ref_company rc ON rc.company_id = dc.company_id
        """)
        to_upsert = []
        for row in links:
            did, cid = row["document_id"], row["company_id"]
            name, ticker = (row["name"] or "").strip(), (row["ticker"] or "").strip()
            low = (" \n".join(chunks_by_doc.get(did, []))).lower()
            nh = safe_regex_count(WORD_BOUNDARY.format(term=re.escape(name.lower())), low) if name else 0
            th = safe_regex_count(WORD_BOUNDARY.format(term=re.escape(ticker.lower())), low) if ticker else 0
            ah = 0
            for al in alias_map.get(cid, []):
                if al: ah += safe_regex_count(WORD_BOUNDARY.format(term=re.escape(al.lower())), low)
            tot = nh + th + ah
            to_upsert.append((did, cid, nh, th, ah, tot, now))
        n = upsert_counts(conn, to_upsert); conn.commit()
        print(f"[ctc] Upserted {n} rows (document_company).")
        return

    # FALLBACK: global scan
    print("[ctc] document_company empty → global scan (name/ticker/aliases).")
    # Precompile patterns
    comp_specs: List[Tuple[int, Optional[str], Optional[str], List[str]]] = []
    for c in companies:
        cid = c["company_id"]
        name = (c["legal_name"] or "").strip()
        ticker = (c["ticker"] or "").strip()
        pat_name   = WORD_BOUNDARY.format(term=re.escape(name.lower())) if name else None
        pat_ticker = WORD_BOUNDARY.format(term=re.escape(ticker.lower())) if ticker else None
        pat_aliases = []
        for al in alias_map.get(cid, []):
            al = (al or "").strip()
            if al: pat_aliases.append(WORD_BOUNDARY.format(term=re.escape(al.lower())))
        comp_specs.append((cid, pat_name, pat_ticker, pat_aliases))

    total_upserts = 0
    processed_docs = 0
    for did in doc_ids:
        processed_docs += 1
        low = (" \n".join(chunks_by_doc.get(did, []))).lower()
        to_upsert = []
        for cid, pat_name, pat_ticker, pat_aliases in comp_specs:
            nh = safe_regex_count(pat_name, low) if pat_name else 0
            th = safe_regex_count(pat_ticker, low) if pat_ticker else 0
            ah = 0
            for pa in pat_aliases: ah += safe_regex_count(pa, low)
            tot = nh + th + ah
            if tot > 0:
                to_upsert.append((did, cid, nh, th, ah, tot, now))
        total_upserts += upsert_counts(conn, to_upsert)
        if processed_docs % 100 == 0:
            conn.commit()
            print(f"[ctc] ... processed {processed_docs}/{len(doc_ids)} docs; {total_upserts} rows upserted")

    conn.commit()
    print(f"[ctc] Upserted {total_upserts} rows (global scan).")

# ----------------- main -----------------

def main():
    print(f"[setup] Opening DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    # 1) Migrate/ensure constraints
    ensure_ref_company(conn)
    ensure_ref_company_alias(conn)
    ensure_company_term_count(conn)

    # 2) Seed
    seed_ref_company(conn)
    seed_aliases(conn)

    # 3) Rebuild counts
    rebuild_counts(conn)

    conn.close()

if __name__ == "__main__":
    try:
        main()
    except sqlite3.OperationalError as e:
        print(f"[ctc] SQLite error: {e}")
        sys.exit(1)
