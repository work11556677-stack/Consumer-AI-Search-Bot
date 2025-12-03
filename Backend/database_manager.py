import re 
import json
import sqlite3 
from typing import Dict, List, Tuple, Optional, Sequence, Any



def db(db_path_main) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path_main, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]

def fetchone(conn, sql, params=(), schema_hint: Optional[str]=None):
    return conn.execute(sql, params).fetchone()

def fetchall(conn, sql, params=(), schema_hint: Optional[str]=None):
    return conn.execute(sql, params).fetchall()

def _table_names(conn: sqlite3.Connection, schema: str = "main") -> set:
    return {r[0] for r in conn.execute(f"SELECT name FROM {schema}.sqlite_master WHERE type='table';").fetchall()}

def has_table(conn: sqlite3.Connection, schema: str, table: str) -> bool:
    return table in _table_names(conn, schema)

def _col_exists(conn, table: str, col: str) -> bool: 
    try: 
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")] 
        return col in cols 
    except Exception: 
        return False
    
def _json_safe_parse(s: Optional[str]) -> Dict[str, Any]:
    if not s: return {}
    try: return json.loads(s)
    except Exception: return {}

def safe_regex_count(pattern: Optional[str], text: str) -> int:
    """Safe case-insensitive regex count with guard against bad patterns."""
    if not pattern or not text:
        return 0
    try:
        return len(re.findall(pattern, text, flags=re.I))
    except re.error:
        return 0
    
def resolve_company_ids(
    conn: sqlite3.Connection,
    cues: List[str],
) -> List[int]:
    """
    Map cues (ticker or name fragments) to company_id via ref_company.
    """
    if not cues:
        return []
    where, params = [], []
    for s in cues:
        s = s.strip().lower()
        if not s:
            continue
        where.append("(LOWER(ticker)=? OR LOWER(legal_name) LIKE ?)")
        params.extend([s, f"%{s}%"])
    if not where:
        return []
    sql = f"SELECT DISTINCT company_id FROM ref_company WHERE {' OR '.join(where)}"
    cur = conn.execute(sql, params)
    rows = [int(r["company_id"]) for r in cur.fetchall()]
    cur.close()
    return rows

def fetch_doc_pool(
    conn: sqlite3.Connection,
    company_ids: List[int],
    limit_pool: int = 200,
) -> List[sqlite3.Row]:
    """
    Precomputed doc pool via company_term_count (static path).
    Shape matches your previous _fetch_doc_pool.
    """
    if not company_ids:
        return []

    have_alias = _col_exists(conn, "company_term_count", "alias_hits")
    extra = ", c.alias_hits" if have_alias else ""

    qmarks = ",".join("?" * len(company_ids))

    sql = f"""
        SELECT 
            d.document_id,
            d.title,
            d.published_at,
            '' AS source_url,
            '' AS source_path,
            c.company_id,
            c.total_hits,
            c.name_hits,
            c.ticker_hits
            {extra}
        FROM company_term_count c
        JOIN document d ON d.document_id = c.document_id
        WHERE c.company_id IN ({qmarks})
        ORDER BY 
            c.total_hits DESC,
            COALESCE(d.published_at, '') DESC,
            c.name_hits DESC, 
            c.ticker_hits DESC
        LIMIT ?
    """
    


    cur = conn.execute(sql, (*company_ids, limit_pool))
    rows = cur.fetchall()
    cur.close()
    return rows

def fetch_all_docs(
    conn: sqlite3.Connection,
    limit_pool: int = 2000,
) -> List[sqlite3.Row]:
    """
    Fetch ALL documents across ALL companies through company_term_count index.
    Matches the shape of fetch_doc_pool() so downstream logic works identically.

    Returns rows with:
        document_id, title, published_at,
        source_url, source_path,
        company_id,
        total_hits, name_hits, ticker_hits,
        alias_hits (if exists)
    """

    have_alias = _col_exists(conn, "company_term_count", "alias_hits")
    extra = ", c.alias_hits" if have_alias else ""

    sql = f"""
        SELECT 
            d.document_id,
            d.title,
            d.published_at,
            '' AS source_url,
            '' AS source_path,
            c.company_id,
            c.total_hits,
            c.name_hits,
            c.ticker_hits
            {extra}
        FROM company_term_count c
        JOIN document d ON d.document_id = c.document_id
        ORDER BY 
            c.total_hits DESC,
            COALESCE(d.published_at, '') DESC,
            c.name_hits DESC,
            c.ticker_hits DESC
        LIMIT ?
    """

    cur = conn.execute(sql, (limit_pool,))
    rows = cur.fetchall()
    cur.close()
    return rows

def fetch_doc_chunks_robust(
    conn: sqlite3.Connection,
    document_id: int,
) -> List[Dict[str, Any]]:
    """
    Return rows with unified keys: page:int, chunk_index:int, text:str
    Tries multiple table/column layouts seen in your DBs.
    """
    candidates = [
        # (table,         text_col,        page_col,         idx_col)
        ("chunk", "text", "page_start", "chunk_index"),
        ("chunk", "chunk_text", "page_start", "chunk_index"),
        ("chunk", "content", "page_no", "chunk_id"),
        ("chunks", "content", "page_no", "chunk_id"),
        ("doc_chunk", "content", "page_no", "chunk_idx"),
        ("main_chunk", "content", "page_no", "chunk_id"),
    ]
    for table, text_c, page_c, idx_c in candidates:
        try:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        except Exception:
            continue
        if {text_c}.issubset(cols) and page_c in cols:
            has_idx = idx_c in cols
            try:
                sql = f"""
                    SELECT
                      COALESCE({page_c}, 1) AS page,
                      {text_c} AS text
                      {', COALESCE(' + idx_c + ', 0) AS chunk_index' if has_idx else ', 0 AS chunk_index'}
                    FROM {table}
                    WHERE document_id = ?
                    ORDER BY COALESCE({page_c}, 999999), {idx_c if has_idx else 'rowid'}
                """
                cur = conn.execute(sql, (document_id,))
                rows = cur.fetchall()
                cur.close()

                out: List[Dict[str, Any]] = []
                for r in rows:
                    t = (r["text"] or "").strip()
                    if not t:
                        continue
                    pg = int(r["page"] or 1)
                    ci = int(r["chunk_index"] or 0)
                    out.append({"page": pg, "chunk_index": ci, "text": t})
                if out:
                    print(
                        f"fetch_doc_chunks_robust: hit table={table} "
                        f"text={text_c} page={page_c} idx={idx_c} -> {len(out)} chunks"
                    )
                    return out
            except Exception as e:
                print(
                    f"fetch_doc_chunks_robust: failed on {table} "
                    f"({text_c},{page_c},{idx_c}): {e}"
                )
                continue
    print("fetch_doc_chunks_robust: no layouts matched; returning []")
    return []

def get_company_id_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
) -> Optional[int]:
    cur = conn.execute(
        "SELECT company_id FROM ref_company WHERE UPPER(ticker)=UPPER(?)",
        (ticker,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return int(row[0])

def dynamic_company_pool(
    conn: sqlite3.Connection,
    company_id: int,
    ticker: str,
    legal_name: str,
    aliases: List[str],
    limit_pool: int = 200,
) -> List[Dict[str, Any]]:
    """
    Runtime-only, NO WRITES. Scans documents for name/ticker/aliases and
    returns a pool of docs shaped like fetch_doc_pool output.
    """
    # candidate docs
    doc_ids: List[int] = []
    if company_id != -1 and has_table(conn, "document_company"):
        try:
            cur = conn.execute(
                "SELECT document_id FROM document_company WHERE company_id=?",
                (company_id,),
            )
            doc_ids = [int(r["document_id"]) for r in cur.fetchall()]
            cur.close()
        except Exception as e:
            print(
                f"dynamic_company_pool: error querying document_company "
                f"for company_id={company_id}: {e}"
            )
            doc_ids = []

    if not doc_ids:
        try:
            cur = conn.execute("SELECT document_id FROM document")
            doc_ids = [int(r["document_id"]) for r in cur.fetchall()]
            cur.close()
        except Exception as e:
            print(f"dynamic_company_pool: failed to list document ids: {e}")
            return []

    print(
        f"dynamic_company_pool: scanning {len(doc_ids)} docs for "
        f"ticker={ticker}, company_id={company_id}"
    )

    # patterns
    name_pat = (
        config.WORD_BOUNDARY.format(term=re.escape(legal_name.lower()))
        if legal_name
        else None
    )
    ticker_pat = config.WORD_BOUNDARY.format(term=re.escape(ticker.lower()))

    alias_pats: List[str] = []
    for al in aliases:
        al = al.strip()
        if not al:
            continue
        alias_pats.append(config.WORD_BOUNDARY.format(term=re.escape(al.lower())))

    hit_rows: List[Dict[str, Any]] = []

    for did in doc_ids:
        chunks = fetch_doc_chunks_robust(conn, did)
        if not chunks:
            continue
        low_text = " ".join(ch["text"] for ch in chunks).lower()

        name_hits = safe_regex_count(name_pat, low_text) if name_pat else 0
        ticker_hits = safe_regex_count(ticker_pat, low_text) if ticker_pat else 0
        alias_hits = 0
        for pat in alias_pats:
            alias_hits += safe_regex_count(pat, low_text)

        total_hits = name_hits + ticker_hits + alias_hits
        if total_hits <= 0:
            continue

        hit_rows.append(
            {
                "document_id": did,
                "company_id": company_id,
                "name_hits": name_hits,
                "ticker_hits": ticker_hits,
                "alias_hits": alias_hits,
                "total_hits": total_hits,
            }
        )

    if not hit_rows:
        print("dynamic_company_pool: no docs with any hits.")
        return []

    # sort & top-k
    hit_rows.sort(
        key=lambda r: (r["total_hits"], r["document_id"]), reverse=True
    )
    top = hit_rows[:limit_pool]

    # enrich to match fetch_doc_pool shape
    pool: List[Dict[str, Any]] = []
    for r in top:
        did = r["document_id"]
        try:
            cur = conn.execute(
                "SELECT document_id, title, published_at, "
                "       '' AS source_url, '' AS source_path "
                "FROM document WHERE document_id=?",
                (did,),
            )
            dmeta = cur.fetchone()
            cur.close()
        except Exception as e:
            print(
                f"dynamic_company_pool: failed to fetch document meta "
                f"for {did}: {e}"
            )
            dmeta = None

        title = ""
        published_at = ""
        source_url = ""
        source_path = ""

        if dmeta:
            keys = dmeta.keys()
            title = (dmeta["title"] or "") if "title" in keys else ""
            published_at = (
                dmeta["published_at"] or ""
                if "published_at" in keys
                else ""
            )
            source_url = (
                dmeta["source_url"] or ""
                if "source_url" in keys
                else ""
            )
            source_path = (
                dmeta["source_path"] or ""
                if "source_path" in keys
                else ""
            )

        pool.append(
            {
                "document_id": did,
                "title": title,
                "published_at": published_at,
                "source_url": source_url,
                "source_path": source_path,
                "company_id": r["company_id"],
                "total_hits": r["total_hits"],
                "name_hits": r["name_hits"],
                "ticker_hits": r["ticker_hits"],
                "alias_hits": r["alias_hits"],
            }
        )

    print(f"dynamic_company_pool: built dynamic pool size={len(pool)}")
    return pool

def get_context_chunks_for_sources(conn, sources_for_prompt):
    """
    Build context text in EXACT SAME ORDER as sources_for_prompt.
    Ensures:
      - pages are sorted
      - S# aligns correctly
      - chunks returned in stable logical order
    """
    context_blocks = []

    for s_idx, src in enumerate(sources_for_prompt, start=1):
        doc_id = src["document_id"]

        pages = src.get("pages", [])

        # IMPORTANT: sort pages
        pages = sorted(set(pages))

        for page in pages:
            rows = conn.execute(
                """
                SELECT text, page_start, page_end, chunk_index
                FROM chunk
                WHERE document_id = ?
                  AND page_start = ?
                ORDER BY chunk_index ASC
                """,
                (doc_id, page)
            ).fetchall()

            for r in rows:
                txt = (r["text"] or "").strip()
                if not txt:
                    continue

                context_blocks.append(
                    f"[S{s_idx} p{page}] {txt}"
                )

    return context_blocks

def print_gen_doc_ids(conn):
    # Resolve the company_id for GEN
    gen_ids = resolve_company_ids(conn, ["GEN"])

    if not gen_ids:
        print("No company_id found for GEN")
        return

    print(f"GEN company_ids={gen_ids}")

    # Fetch documents
    pool = fetch_doc_pool(conn, gen_ids, limit_pool=5000)
    print(f"Found {len(pool)} GEN documents")

    # Print all document IDs
    doc_ids = [int(r["document_id"]) for r in pool]
    print("GEN document_ids:", doc_ids)

def get_document_fields(
    conn: sqlite3.Connection,
    document_id: int,
) -> Dict[str, Any]:
    """
    Fetch title/published_at/file_uri/meta for a document_id from the `document` table,
    and derive source_url/source_path from the meta JSON.

    Returns a dict with keys:
      - title
      - published_at
      - file_uri
      - mime_type
      - source_url
      - source_path
      - meta (decoded JSON dict, or {})
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT title, published_at, file_uri, mime_type, meta
        FROM document
        WHERE document_id = ?
        """,
        (document_id,),
    )
    row = cur.fetchone()
    if not row:
        # safe empty defaults if doc not found
        return {
            "title": "",
            "published_at": "",
            "file_uri": "",
            "mime_type": "",
            "source_url": "",
            "source_path": "",
            "meta": {},
        }

    title, published_at, file_uri, mime_type, meta_json = row

    meta: Dict[str, Any] = {}
    if meta_json:
        try:
            meta = json.loads(meta_json) if isinstance(meta_json, str) else meta_json
        except Exception:
            meta = {}

    # Fill from meta as fallbacks
    title = title or meta.get("page_title") or meta.get("title") or ""
    published_at = (
        published_at
        or meta.get("published_at")
        or meta.get("date")
        or ""
    )

    # "file_uri" is usually the raw path or URL stored in the document table
    # For source_url we try meta first, then file_uri
    source_url = (
        meta.get("source_url")
        or meta.get("url")
        or file_uri
        or ""
    )

    # source_path lives in meta (e.g. local filesystem path or relative path)
    source_path = (
        meta.get("source_path")
        or meta.get("path")
        or ""
    )

    return {
        "title": title or "",
        "published_at": published_at or "",
        "file_uri": file_uri or "",
        "mime_type": mime_type or "",
        "source_url": source_url or "",
        "source_path": source_path or "",
        "meta": meta,
    }