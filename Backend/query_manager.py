import config
import openai_manager
import database_manager


import os
import re
import html
import json
import math
import sqlite3
import numpy as np
from pathlib import Path
from fastapi import Query
from datetime import datetime
from urllib.parse import quote
from urllib.parse import quote as urlquote, urlparse, unquote
from typing import Dict, List, Tuple, Optional, Sequence, Any



## random helpers 
def pubdate(row):
        d = row["published_at"]
        return d if d and d.strip() else "0000-00-00"

def append_qa_output(question: str, summary_md: str, citations: list, references: list):
    """
    Append the query, answer bullets, citations JSON, and sources used
    to a human-readable output file.
    """

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # FLOW: Format Sources Used section (only those actually cited)
    sources_lines = []
    for ref in references:
        title = ref.get("title", "")
        pages = ref.get("pages", [])
        if pages:
            pages_str = ", ".join(f"p.{p}" for p in pages)
        else:
            pages_str = "p.?"

        # We don't store the quote in references object -> fall back to ""
        quote = ref.get("quote", "").strip()

        if quote:
            sources_lines.append(f"- {title} — {pages_str} — \"{quote}\"")
        else:
            sources_lines.append(f"- {title} — {pages_str}")

    sources_formatted = "\n".join(sources_lines)

    # FLOW: Build final block
    block = f"""
    =====================
    {ts}
    =====================

    Query:
    {question}

    Answer:
    {summary_md}

    Sources Used:
    {sources_formatted}

    -------------------------------------------------------------

    """.lstrip()

    # FLOW: Append to file
    with open(config.OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(block)

    print(f"[log] Output appended to {config.OUTPUT_FILE}")

def parse_sources_from_llm_output(llm_output: str):
    """
    Extracts the 'Sources' section from the LLM output and returns:
    [
      {"title": "...", "pages": [1,2], "quote": "text"},
      ...
    ]
    """
    references = []

    # FLOW: Find the Sources section
    m = re.search(r"Sources\s*(.+)$", llm_output, flags=re.S | re.I)
    if not m:
        return []

    sources_block = m.group(1).strip()

    # FLOW: Extract each bullet line
    lines = [
        ln.strip() for ln in sources_block.splitlines()
        if ln.strip().startswith("- ")
    ]

    for ln in lines:
        # Pattern: - Title — p.1, p.2 — "quote here"
        pattern = r"^- (.+?)\s+—\s+p\.([0-9,\s]+)(?:\s+—\s+\"(.+?)\")?$"
        m2 = re.match(pattern, ln)
        if not m2:
            continue

        title = m2.group(1).strip()

        pages_raw = m2.group(2)
        pages = [int(p.strip()) for p in pages_raw.split(",") if p.strip().isdigit()]

        quote = m2.group(3) if m2.group(3) else ""

        references.append({
            "title": title,
            "pages": pages,
            "quote": quote
        })

    return references



# =========================================
# Step 1: Parse input query AND (Extract Known Company OR (Use LLM Extract Company AND Search database))
# =========================================
def classify_use_case(q: str) -> Dict[str, Any]:
    ql = q.lower()
    # FLOW: basic heuristcs 
    company_hit = any(t.lower() in ql for t in config.COMPANY_TERMS)
    macro_hit = any(w in ql for w in [
        "forecast", "outlook", "drivers", "industry", "rate cut", "rate cuts",
        "savings rate", "online penetration", "australian dollar", "inflation",
        "volume growth", "themes", "macro", "sector", "market-wide", "retail spending"
    ])

    if company_hit:
        heuristic = "use_case_1"
    elif macro_hit:
        heuristic = "use_case_2"
    else:
        heuristic = None

    # FLOW: sys prompt
    system = (
        "You are a retail research classifier.\n\n"
        "You are given a coverage_universe which is a list of company names. "
        "Your tasks are:\n\n"
        "1. Classify the question as:\n"
        "   - use_case_1: company-specific, OR\n"
        "   - use_case_2: sector / macro.\n"
        "   HARD RULE: If ANY company is mentioned in the question, use_case_1.\n\n"
        "2. For use_case_2 ONLY: identify which companies from coverage_universe are relevant to the "
        "   sector or category mentioned in the question. You MUST:\n"
        "   - Include ONLY companies whose *primary business* is in that sector.\n"
        "   - EXCLUDE companies with only secondary, marginal, historical, or incidental exposure.\n"
        "   - If 'furniture' is mentioned, return only companies primarily in furniture retailing "
        "     (e.g., Nick Scali, Adairs, Temple & Webster) and NOT companies like Super Retail Group.\n"
        "   - If the sector is broad across all retail, return [].\n\n"
        "3. Extract key_terms: the meaningful conceptual terms from the question.\n\n"
        "Return JSON only: {use_case, confidence, reason, related_companies, key_terms}."
    )



    # FLOW: call llm
    try:
        r = openai_manager.CLIENT.chat.completions.create(
            model=config.CLASSIFY_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": f"coverage_universe: {config.ASX_COMPANIES.values()}"},
                {"role": "user", "content": q},
                {"role": "user", "content": f"Heuristic hint: {heuristic or 'unknown'}"}
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )

        js = json.loads(r.choices[0].message.content)

        uc = js.get("use_case") or heuristic or "use_case_1"
        conf = js.get("confidence") or 0.5
        reason = (js.get("reason") or "").strip()

        # enforce routing rule: if company appears we always have case 1 
        if company_hit and uc != "use_case_1":
            uc = "use_case_1"
            reason = (reason + " | forced: company detected").strip()
            js["related_companies"] = []  # override: company-level questions can't have related list

        # Guarantee fields exist
        related = js.get("related_companies", [])
        key_terms = js.get("key_terms", [])

    except Exception as e:
        print(f"query_manager:classify_use_case:ERROR {e} -> fallback path used")
        uc = "use_case_1" if company_hit else (heuristic or "use_case_2" if macro_hit else "use_case_1")
        conf = 0.4
        reason = "fallback heuristic (LLM error)"
        related = []
        key_terms = []

    # FLOW: final formatted output
    out = {
        "use_case": uc,
        "confidence": conf,
        "reason": reason,
        "heuristic": heuristic,
        "company_hit": company_hit,
        "related_companies": related,
        "key_terms": key_terms,
    }

    print(f"query_manager:classify_use_case:DEBUG {config.CLASSIFY_MODEL} -> {out}")
    return out

def handle_use_case_1(q, tokens, tickers, conn):
    """
    We extract the key companies mentioned in the querstoin and find the reports that mention them the most. 
    If the company is not known, use llm to generate aliases and search datbase. 

    Returns: pool, tickers, extra_terms
    """

    # FLOW: extract company from qeury, and resovle into aliases, etc
    cues: List[str] = []
    if tickers: 
        cues.extend(_aliases_for_tickers(tickers))
    else:
        # no obvious ticker: scan they query for any known aliases or legal name 
        flat = set()
        for tk, arr in config.ALIASES.items():
            flat.update(arr)
        flat.update(config.ASX_COMPANIES.values())
        flat.update(config.ASX_COMPANIES.keys())
        ql = q.lower()
        cues.extend([a for a in flat if a and a.lower() in ql])


    print(f"query_manager:handle_use_case_1:DEBUG: cues(len)={len(cues)} sample={cues[:10]}")

    # FLOW: from company extraction, retrieve matching docs 
    company_ids = database_manager.resolve_company_ids(conn, cues)
    print(f"query_manager:handle_use_case_1:DEBUG: company_ids: {company_ids}")

    if not company_ids and tickers:
        company_ids = database_manager.resolve_company_ids(conn, tickers)
        print(f"query_manager:handle_use_case_1:DEBUG: fallback company_ids via tickers={company_ids}")

    # Pre-compute non-company terms for ranking (used later regardless of mode)
    company_words = set(w.lower() for w in cues + tickers)
    extra_terms = [t for t in tokens if t and t.lower() not in company_words]
    print(f"query_manager:handle_use_case_1:DEBUG: extra_terms={extra_terms}")

    # FLOW: 
    #          If there is valid company_ids (we have a known company e.g. wow, then fetch doc_pool)
    #          ELSE: If there is no known company found -> use LLM to determine company (e.g temu not in known compnaies, but still case 1 question)
    #          and build a dynamic doc pool at inference time.
    #          If that also fails (no docs), RETURN and do not continue.
    if company_ids:
        # Static path: use precomputed company_term_count
        pool = database_manager.fetch_doc_pool(conn, company_ids, limit_pool=200)
        print(f"query_manager:handle_use_case_1:DEBUG: pool_size={len(pool)} (static company_term_count path)")
    else:
        # Dynamic path: let llm pick a company, then scan docs
        print("query_manager:handle_use_case_1:DEBUG: No company_ids from static extraction -> entering dynamic LLM company path.")
        pool = dynamic_company(q, conn, limit_pool=200)
        print(f"query_manager:handle_use_case_1:DEBUG: pool_size={len(pool)} (dynamic LLM company path)")

        # If dynamic path also fails -> abort (no reports to summarise)
        if not pool:
            print("query_manager:handle_use_case_1:DEBUG: No reports found via static or dynamic company extraction -> aborting.")
            return  # early exit; caller will see None


    return pool, tickers, extra_terms

def handle_use_case_2(q, tokens, out, conn):
    """
    Hybrid workflow for sector/macro questions:
    Unlike case 1, just using compnaies, 
        we use the llm to extract both related companies (if any) and key terms, and we search the datbase for reports containing them. 

    Always returns: pool, tickers, extra_terms
    """

    related_companies = out["related_companies"]
    key_terms = out["key_terms"]

    # FLOW: Convert related company names -> tickers
    tickers = [
        ticker
        for ticker, name in config.ASX_COMPANIES.items()
        if name in related_companies
    ]

    print(f"query_manager:handle_use_case_2:DEBUG: tickers={tickers}, key_terms={key_terms}")

    # FLOW: Build cues only from ticker aliases (not keywords)
    cues = []
    if tickers:
        cues.extend(_aliases_for_tickers(tickers))

    # FLOW: Resolve company_ids 
    company_ids = []
    if tickers:
        company_ids = database_manager.resolve_company_ids(conn, tickers)
        print(f"query_manager:handle_use_case_2:DEBUG: company_ids={company_ids}")

    # FLOW: Build initial candidate pool
    #       using company ids, and also getting all docs for keyword matching to query case 2 terms
    if company_ids:
        pool = database_manager.fetch_doc_pool(conn, company_ids, limit_pool=500)
        print(f"query_manager:handle_use_case_2:DEBUG: pool_size={len(pool)} (filtered by company)")
    else:
        pool = database_manager.fetch_all_docs(conn, limit_pool=2000)
        print(f"query_manager:handle_use_case_2:DEBUG: pool_size={len(pool)} (full corpus)")

    if not pool:
        print("query_manager:handle_use_case_2:DEBUG: Empty pool -> dynamic fallback")
        pool = dynamic_company(q, conn, limit_pool=500)

    if not pool:
        print("query_manager:handle_use_case_2:DEBUG: docs found -> abort")
        return None, None, None

    # Build extra_terms (tokens minus company words)
    company_words = set(w.lower() for w in cues + tickers)
    extra_terms = [t for t in tokens if t and t.lower() not in company_words]

    for kt in key_terms:
        if kt not in extra_terms:
            extra_terms.append(kt)

    print(f"query_manager:handle_use_case_2:DEBUG: extra_terms={extra_terms}")

    # FLOW: Keyword Sorting 
    if extra_terms:
        def term_score(text, terms):
            t = text.lower()
            return sum(t.count(term.lower()) for term in terms)

        scored = []
        for row in pool:
            r = dict(row)
            text = r.get("title", "") + " " + r.get("clean_text", "")
            r["extra_term_score"] = term_score(text, extra_terms)
            scored.append(r)

        pool = sorted(scored, key=lambda r: r["extra_term_score"], reverse=True)

    return pool, tickers, extra_terms

def _parse_query(q: str):
    return [t for t in re.split(r"[^A-Za-z0-9\+\&]+", (q or "").strip()) if t]

def _guess_tickers(tokens):
    ups = [t.upper() for t in tokens]
    return [tk for tk in config.ASX_COMPANIES.keys() if tk in ups]

def _aliases_for_tickers(tickers):
    out = []
    for tk in tickers:
        out.extend(config.ALIASES.get(tk, []))
        lname = config.ASX_COMPANIES.get(tk)
        if lname:
            out.append(lname)
    out.extend(tickers)  # include literal ticker strings
    # de-dupe preserving order
    seen, res = set(), []
    for s in out:
        if s not in seen:
            seen.add(s); res.append(s)
    return res

def llm_determine_company(user_query: str) -> Optional[Dict[str, Any]]:
    """
    Open-world company detector.

    Goal:
      Given an arbitrary user query, identify the ONE primary real-world company
      or brand the query is mainly about (e.g., "Amazon", "Shein", "TikTok").

    Returns:
      {
        "company_name": "Amazon.com, Inc.",   # long / formal name if known
        "short_name": "Amazon",              # short name likely to appear in text
        "aliases": [                         # other surface forms we should search for
          "Amazon", "Amazon Australia", "amazon.com", "Amazon Prime", ...
        ]
      }
      or None if no clear company is mentioned in the query.
    """
    system = (
        "You are a company/brand name recogniser.\n"
        "Given a user query, identify the ONE main real-world company or consumer brand "
        "that the user is talking about.\n"
        "You are NOT restricted to any preset list; use your knowledge of global companies.\n"
        "Return ONLY a JSON object and nothing else.\n"
        "If the query does not clearly refer to a specific company/brand, return:\n"
        '{"company_name": null, "short_name": null, "aliases": []}.'
    )

    user = f"""
    User query:
    {user_query}

    Task:
    1. Decide if this query is primarily about a single company or consumer-facing brand
    (e.g., "Amazon", "Shein", "TikTok", "Temu", "Costco", "Bunnings", "Kmart", etc.).
    2. If YES, output JSON in this exact shape:

    {{
    "company_name": "<Full or formal name if you know it>",  // e.g. "Amazon.com, Inc."
    "short_name": "<short display name>",                    // e.g. "Amazon"
    "aliases": [
        "<common way the company is referred to in text>",
        "<brand/store names it operates under>",
        "<plausible spelling variants>",
        ...
    ]
    }}

    Guidelines:
    - short_name should be what usually appears as a single token or 1–2 words (e.g. "Amazon").
    - aliases should include things like:
    - direct brand names (e.g. "Amazon", "Amazon Australia")
    - domain-style names (e.g. "amazon.com")
    - key consumer-facing products if they might be used as a stand-in for the company
        (e.g. "Amazon Prime", "Prime Video" for Amazon).
    - Do NOT include generic words like "online", "shopping", "app" as aliases.

    3. If there is NO clear single company or brand:
    return exactly:
    {{"company_name": null, "short_name": null, "aliases": []}}

    IMPORTANT:
    - Output MUST be valid JSON only (no backticks, no explanation).
    """

    resp = openai_manager.CLIENT.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.0,
    )
    raw = (resp.choices[0].message.content or "").strip()

    # wrapped JSON 
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if m:
        raw = m.group(0)

    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"query_manager:llm_determine_company:DEBUG: JSON parse error: {e} raw={raw[:200]!r}")
        return None

    company_name = (data.get("company_name") or "").strip() if data.get("company_name") else ""
    short_name = (data.get("short_name") or "").strip() if data.get("short_name") else ""
    aliases_raw = data.get("aliases", []) or []

    if not company_name and not short_name:
        print("query_manager:llm_determine_company:DEBUG: model returned no company (both fields empty/null).")
        return None

    aliases: List[str] = []
    for a in aliases_raw:
        if isinstance(a, str):
            s = a.strip()
            if s:
                aliases.append(s)

    # Make sure short_name appears in aliases for searching
    if short_name and short_name not in aliases:
        aliases.insert(0, short_name)

    print(
        f"query_manager:llm_determine_company:DEBUG: company_name={company_name!r}, "
        f"short_name={short_name!r}, aliases={aliases}"
    )

    return {
        "company_name": company_name or short_name,
        "short_name": short_name or company_name,
        "aliases": aliases,
    }

def dynamic_company(
    user_query: str,
    conn: sqlite3.Connection,
    limit_pool: int = 200,
) -> List[Dict[str, Any]]:
    """
    Dynamic, inference-time document search for an off-book company.

    Used when:
      - Static ASX-based company detection finds NO company_ids.
      - We still want to answer by letting the LLM identify ANY real-world company
        from the query (e.g. "Amazon"), then scanning documents at runtime.

    Behaviour:
      - Calls llm_determine_company(user_query) to get company_name + aliases.
      - Scans all documents' chunks (via database_manager.fetch_doc_chunks_robust)
        with word-boundary regexes for:
          * company_name      -> name_hits
          * short_name        -> ticker_hits (used as a second "name" bucket)
          * aliases           -> alias_hits
      - Returns a doc pool shaped like the static pool from company_term_count:
          {
            "document_id", "title", "published_at",
            "source_url", "source_path",
            "company_id",   # always -1 for off-book companies
            "total_hits", "name_hits", "ticker_hits", "alias_hits"
          }
      - NO writes to the database.
    """
    info = llm_determine_company(user_query)
    if not info:
        print("query_manager:dynamic_company:DEBUG: LLM could not confidently identify any company from query.")
        return []

    company_name = (info.get("company_name") or "").strip()
    short_name = (info.get("short_name") or "").strip()
    aliases = info.get("aliases") or []

    lower_name = company_name.lower()
    lower_short = short_name.lower()

    # Get helpers/constants from database_manager, with safe fallbacks
    boundary_template = getattr(
        database_manager,
        "WORD_BOUNDARY",
        r"(?<![A-Za-z0-9]){term}(?![A-Za-z0-9])",
    )
    safe_count = getattr(
        database_manager,
        "safe_regex_count",
        lambda pattern, text: len(re.findall(pattern, text, flags=re.I)) if pattern and text else 0,
    )

    name_pat = (
        boundary_template.format(term=re.escape(lower_name))
        if lower_name
        else None
    )
    short_pat = (
        boundary_template.format(term=re.escape(lower_short))
        if lower_short and lower_short != lower_name
        else None
    )

    alias_pats: List[str] = []
    for a in aliases:
        if not isinstance(a, str):
            continue
        s = a.strip().lower()
        if not s:
            continue
        # avoid duplicating the main patterns
        if s == lower_name or s == lower_short:
            continue
        alias_pats.append(
            boundary_template.format(term=re.escape(s))
        )

    # Candidate document set: for off-book companies we just scan all docs
    try:
        cur = conn.execute("SELECT document_id FROM document")
        doc_ids = [int(r["document_id"]) for r in cur.fetchall()]
        cur.close()
    except Exception as e:
        print(f"query_manager:dynamic_company:DEBUG: failed to list document ids: {e}")
        return []

    print(
        f"query_manager:dynamic_company:DEBUG: scanning {len(doc_ids)} docs for "
        f"company_name={company_name!r}, short_name={short_name!r}"
    )

    hit_rows: List[Dict[str, Any]] = []

    for did in doc_ids:
        chunks = database_manager.fetch_doc_chunks_robust(conn, did)
        if not chunks:
            continue

        low_text = " ".join(ch["text"] for ch in chunks).lower()
        if not low_text:
            continue

        name_hits = safe_count(name_pat, low_text) if name_pat else 0
        ticker_hits = safe_count(short_pat, low_text) if short_pat else 0
        alias_hits = 0
        for pat in alias_pats:
            alias_hits += safe_count(pat, low_text)

        total_hits = name_hits + ticker_hits + alias_hits
        if total_hits <= 0:
            continue

        hit_rows.append(
            {
                "document_id": did,
                "company_id": -1,  # off-book / runtime-only company
                "name_hits": name_hits,
                "ticker_hits": ticker_hits,
                "alias_hits": alias_hits,
                "total_hits": total_hits,
            }
        )

    if not hit_rows:
        print("query_manager:dynamic_company:DEBUG: no docs with any hits for this off-book company.")
        return []

    # Sort by relevance: total_hits desc, then document_id desc
    hit_rows.sort(
        key=lambda r: (r["total_hits"], r["document_id"]), reverse=True
    )
    top = hit_rows[:limit_pool]

    # Enrich with basic document metadata to match static pool shape
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
            print(f"query_manager:dynamic_company:DEBUG: failed to fetch document meta for {did}: {e}")
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
                "company_id": r["company_id"],        # -1
                "total_hits": r["total_hits"],
                "name_hits": r["name_hits"],
                "ticker_hits": r["ticker_hits"],
                "alias_hits": r["alias_hits"],
            }
        )

    print(f"query_manager:dynamic_company:DEBUG: built dynamic off-book pool size={len(pool)}")
    return pool



# =========================================
# Step 2: fetch and rank docs that relate to company 
# =========================================
def get_doc_path_date(conn, document_id):
    """
    Load meta JSON from document table, extract absolute_path,
    find a YYMMDD date in the path, return datetime object.
    """
    cur = conn.cursor()
    cur.execute("SELECT meta FROM document WHERE document_id=?", (document_id,))
    row = cur.fetchone()
    if not row or not row[0]:
        return None

    try:
        meta = json.loads(row[0])
    except:
        return None

    abs_path = meta.get("absolute_path")
    if not abs_path:
        return None

    # Locate any YYMMDD token in the filename/path
    m = config.DATE_RE.search(abs_path)
    if not m:
        return None

    yymmdd = m.group(1)

    # Interpret YYMMDD -> datetime
    try:
        # 20xx assumption
        full_date = datetime.strptime("20" + yymmdd, "%Y%m%d")
    except:
        return None

    return full_date

def _score_with_extra_terms(rows, extra_terms):
    terms = [t.lower() for t in extra_terms if t]
    def parse_dt(s):
        if not s: return datetime.min
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d"):
            try: return datetime.strptime(s[:19], fmt)
            except: pass
        return datetime.min
    scored = []
    for r in rows:
        base = float(r["total_hits"] or 0)
        title = (r["title"] or "").lower()
        url = (r["source_url"] or "").lower()
        bonus = 0.0
        for t in terms:
            if t in title: bonus += 2.0
            if t in url:   bonus += 1.0
        scored.append((r, base + bonus, parse_dt(r["published_at"])))
    scored.sort(key=lambda z: (z[1], z[2]), reverse=True)
    # pick distinct docs
    out, seen = [], set()
    for r, _, _ in scored:
        did = int(r["document_id"])
        if did in seen: continue
        seen.add(did); out.append(r)
    return out

def _safe_key(row, key):
    # sqlite3.Row supports "in" for keys
    return row[key] if key in row.keys() else None

def _fetch_doc_chunks_robust(conn: sqlite3.Connection, document_id: int) -> list[dict]:
    """
    Return rows with unified keys: page:int, chunk_index:int, text:str
    Tries multiple table/column to accomodate my past DB schemas.
    """
    candidates = [
       # (table,           text_col,        page_col,         idx_col)
        ("chunk",         "text",          "page_start",     "chunk_index"),
        ("chunk",         "chunk_text",    "page_start",     "chunk_index"),
        ("chunk",         "content",       "page_no",        "chunk_id"),
        ("chunks",        "content",       "page_no",        "chunk_id"),
        ("doc_chunk",     "content",       "page_no",        "chunk_idx"),
        ("main_chunk",    "content",       "page_no",        "chunk_id"),
    ]
    for table, text_c, page_c, idx_c in candidates:
        # verify table + columns exist
        try:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        except Exception:
            continue
        if {text_c}.issubset(cols) and page_c in cols:
            # idx is optional
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
                rows = conn.execute(sql, (document_id,)).fetchall()
                out = []
                for r in rows:
                    t = (r["text"] or "").strip()
                    if not t:
                        continue
                    pg = int(r["page"] or 1)
                    ci = int(r["chunk_index"] or 0)
                    out.append({"page": pg, "chunk_index": ci, "text": t})
                if out:
                    print(f"query_manager:_fetch_doc_chunks_robust:DEBUG: hit table={table} text={text_c} page={page_c} idx={idx_c} -> {len(out)} chunks")
                    return out
            except Exception as e:
                print(f"query_manager:_fetch_doc_chunks_robust:DEBUG: failed on {table} ({text_c},{page_c},{idx_c}): {e}")
                continue
    print("query_manager:_fetch_doc_chunks_robust:DEBUG: no layouts matched; returning []")
    return []

def _build_context_blocks(
    conn: sqlite3.Connection,
    refs: List[Dict[str, Any]],
    max_chunks_per_doc: int = 12,
    max_chars_per_doc: int = 8000,
) -> tuple[list[str], list[dict]]:
    """
    Build context text blocks and a 'sources_for_prompt' list
    from the ranked refs.

    - For each ref/document:
        * Fetch up to `max_chunks_per_doc` chunks from the `chunk` table.
        * Concatenate their text into one context block (capped at `max_chars_per_doc`).
        * Track page numbers from page_start/page_end.
    - sources_for_prompt[i] corresponds to S(i+1) in the LLM prompt/output.

    Returns:
        (context_blocks, sources_for_prompt)
        where:
          context_blocks: List[str]
          sources_for_prompt: List[Dict[str, Any]]
            with keys:
              - document_id
              - title
              - published_at
              - pages       (sorted unique list of ints)
              - db          ("main")
              - source_path (absolute/derived path from refs)
              - company_id, total_hits, name_hits, ticker_hits, alias_hits (if present)
    """
    context_blocks: List[str] = []
    sources_for_prompt: List[Dict[str, Any]] = []

    cur = conn.cursor()

    for ref in refs:
        doc_id = ref["document_id"]

        # -----------------------------
        # 1) Fetch chunks for this doc
        # -----------------------------
        cur.execute(
            """
            SELECT text, section, chunk_index, page_start, page_end
            FROM chunk
            WHERE document_id = ?
            ORDER BY chunk_index ASC
            LIMIT ?
            """,
            (doc_id, max_chunks_per_doc),
        )
        rows = cur.fetchall()
        if not rows:
            # no chunks -> skip this doc
            continue

        # -----------------------------
        # 2) Build context text + pages
        # -----------------------------
        pieces: List[str] = []
        pages_set = set()

        for text, section, chunk_index, page_start, page_end in rows:
            if not text:
                continue

            # Track pages
            if isinstance(page_start, int):
                pages_set.add(page_start)
            if isinstance(page_end, int):
                pages_set.add(page_end)

            # Simple formatting: include section header if present
            if section:
                pieces.append(f"[{section}]\n{text}")
            else:
                pieces.append(text)

        if not pieces:
            # no usable text -> skip
            continue

        # Join pieces and cap length to avoid overlong prompts
        full_text = "\n\n".join(pieces)
        if len(full_text) > max_chars_per_doc:
            full_text = full_text[:max_chars_per_doc] + "\n\n[...]"

        # Optional header for readability in the prompt
        header = f"Document: {ref.get('title') or ''} (id={doc_id}, published={ref.get('published_at') or ''})"
        block = header + "\n\n" + full_text
        context_blocks.append(block)

        # -----------------------------
        # 3) Build source entry for this doc
        # -----------------------------
        pages = sorted(p for p in pages_set if isinstance(p, int)) or [1]

        src_entry: Dict[str, Any] = {
            "document_id": doc_id,
            "title": ref.get("title") or "",
            "published_at": ref.get("published_at") or "",
            "pages": pages,
            "db": "main",
            # 👇 crucial: pass through for worker's PDF path mapping
            "source_path": ref.get("source_path", ""),
            # keep some useful metadata around if you need it in prompts/UI
            "company_id": ref.get("company_id"),
            "total_hits": ref.get("total_hits"),
            "name_hits": ref.get("name_hits"),
            "ticker_hits": ref.get("ticker_hits"),
        }
        if "alias_hits" in ref:
            src_entry["alias_hits"] = ref["alias_hits"]

        sources_for_prompt.append(src_entry)

    return context_blocks, sources_for_prompt

# =========================================
# Step 3: Final LLM output and formatting
# =========================================
def _bullets_to_html(md_text: str) -> str:
    items = []
    for ln in (md_text or "").splitlines():
        s = ln.strip()
        if s.startswith(("-", "*", "•")):
            items.append(f"<li>{html.escape(s.lstrip('-*• ').strip())}</li>")
    return f"<ul>{''.join(items)}</ul>" if items else ""

def build_doc_link_from_meta(meta_json: str, page: int | None = None) -> str | None:
    """
    Build a /file/... URL from document.meta JSON.
    - DOCX absolute_path -> prefer sibling PDF (same base name)
    - If that PDF doesn't exist, try an "MST " + filename variant.
    - Returns a URL under our mounted static routes (/file/Docx_Retail or /file/Chart_Packs)
    """
    try:
        meta = json.loads(meta_json) if isinstance(meta_json, str) else (meta_json or {})
    except Exception:
        meta = {}

    abs_path = (meta.get("absolute_path") or "").replace("\\", "/")
    if not abs_path:
        return None

    # Figure out which mount to use and the rel path under that mount
    mount = None
    rel_after_root = None
    if "Docx Retail" in abs_path:
        mount = "/file/Docx_Retail"
        rel_after_root = abs_path.split("Docx Retail", 1)[1].lstrip("/")
        fs_root = config.DOCX_RETAIL_PATH  # mounted in app.mount("/file/Docx_Retail", ...)

    else:
        return None

    # ensure .pdf (docx->pdf)
    base_no_ext, _ = os.path.splitext(rel_after_root)
    candidate_rel = base_no_ext + ".pdf"

    # Check filesystem to see which filename actually exists
    fs_candidate = os.path.join(fs_root, candidate_rel.replace("/", os.sep))
    final_rel = candidate_rel

    if not os.path.exists(fs_candidate) and mount == "/file/Docx_Retail":
        # try "MST " + filename (same folder)
        dirname, fname = os.path.split(candidate_rel)
        alt_rel = os.path.join(dirname, f"MST {fname}").replace("\\", "/")
        fs_alt = os.path.join(fs_root, alt_rel.replace("/", os.sep))
        if os.path.exists(fs_alt):
            final_rel = alt_rel

    url = f"{mount}/{final_rel}"
    if page:
        url += f"#page={int(page)}"
    return url

def _link_for_citation(sources: list[dict], S: int, page: int, quote: str) -> str:
    """
    Prefer deep-linking to the section viewer (/doc/.../context?...) when available,
    passing page + quote so the view can offer a one-click 'Open PDF at page & search'.
    Otherwise, fall back to a /file/...#page=N&search=... URL.
    """
    try:
        if not (1 <= S <= len(sources)):
            return ""
        src = sources[S - 1]
        q = urlquote((quote or "")[:120])

        # FLOW: Section-first (context view)
        ctx = src.get("context_url")
        if ctx:
            join = "&" if "?" in ctx else "?"
            return f"{ctx}{join}view=html&page={int(page)}&quote={q}"

        # FLOW: Fallback: PDF page + search
        url = src.get("url") or build_doc_link_from_meta(src.get("meta", "{}"), page)
        if not url:
            return ""
        if "#page=" in url:
            base, frag = url.split("#", 1)
            sep = "&" if frag else ""
            return f"{base}#{frag}{sep}search={q}"
        if url.startswith("/view/"):
            join = "&" if "?" in url else "?"
            return f"{url}{join}search={q}"
        join = "#" if "#" not in url else ""
        return f"{url}{join}search={q}"
    except Exception:
        return ""

def _html_with_clickable_citations(md_bullets: str, sources: list[dict]) -> str:
    """
    Render bullet Markdown -> HTML list, replacing [S# pN "quote"] with anchors.
    """
    html_ul = _bullets_to_html(md_bullets)
    def repl(m: re.Match) -> str:
        S = int(m.group("S"))
        page = int(m.group("page"))
        quote = m.group("quote")
        href = _link_for_citation(sources, S, page, quote)
        label = f"S{S} p{page}"
        if not href:
            return f"[{label}]"
        return f'<a href="{html.escape(href)}" target="_blank" rel="noopener noreferrer">[{html.escape(label)}]</a>'
    return config._CIT_MARK.sub(repl, html_ul)

def markdown_to_html(md: str, link_map: dict | None = None) -> str:
    """
    Convert lightweight Markdown (bullets, bold, italics, code, links) to HTML.
    Additionally, hyperlink matching titles in a 'Read more' block.
    """
    if not md:
        return ""
    if link_map is None:
        link_map = {}

    def esc(s): return html.escape(s, quote=True)

    lines = md.strip().splitlines()
    html_out, in_list = [], False

    def close_list():
        nonlocal in_list
        if in_list:
            html_out.append("</ul>")
            in_list = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            close_list()
            continue

        # Detect 'Read more:' header
        if stripped.lower().startswith("read more"):
            close_list()
            html_out.append("<h4>Read more</h4>")
            continue

        # Bullet lines
        if re.match(r"^[-*•]\s+", stripped):
            if not in_list:
                html_out.append("<ul>")
                in_list = True
            content = re.sub(r"^[-*•]\s+", "", stripped)
        else:
            close_list()
            content = stripped

        # Try to hyperlink if the start matches a known title
        for title, meta in link_map.items():
            if content.lower().startswith(title.lower()):
                url = meta["url"]
                page = meta.get("page")
                label = esc(content)
                content = f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{label}</a>'
                break

        # Inline markdown formatting
        content = esc(content)
        content = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", content)
        content = re.sub(r"(^|[\s(])\*([^*\n]+)\*", r"\1<em>\2</em>", content)
        content = re.sub(r"`([^`]+)`", r"<code>\1</code>", content)
        content = re.sub(
            r"\[([^\]]+)\]\((https?://[^\s)]+|/[^\s)]+)\)",
            r'<a href="\2" target="_blank" rel="noopener noreferrer">\1</a>',
            content
        )

        # Wrap line
        if in_list:
            html_out.append(f"<li>{content}</li>")
        else:
            html_out.append(f"<p>{content}</p>")

    close_list()
    return "".join(html_out)

def llm_summarize_persona(
    conn,
    context_blocks: List[str],
    user_query: str,
    use_case: str,
    sources_for_prompt: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Persona summary with STRICT inline citation markers:
      [S# pPAGE "SHORT QUOTE"] at the END of each bullet.
    Also emits a machine-readable CITATIONS(JSON) block we can parse.
    """
    # FLOW: Build candidate list for the model (titles + available pages)
    cand_lines = []
    for i, s in enumerate(sources_for_prompt, 1):
        pages = ", ".join([f"p.{p}" for p in (s.get("pages") or [s.get("page") or 1])][:12]) or "p.1"
        cand_lines.append(f"{i}. {s['title']} — {pages}")
    candidates_block = "\n".join(cand_lines) if cand_lines else "No candidates."

    safe_blocks = context_blocks # _budget_texts(context_blocks)
    context_blocks = database_manager.get_context_chunks_for_sources(conn, sources_for_prompt)
    sources_text = "\n\n".join(context_blocks)


    base_persona = (
        "Persona: CEO-brief writer for Australian retail. Use ONLY the provided internal context.\n"
        "Priorities: newest first; crisp, quantified forward-looking bullets. No investment advice."
        if use_case != "use_case_2" else
        "Persona: Sector/macro brief writer. Use ONLY the provided internal context.\n"
        "Priorities: newest first; sector-level quantified bullets. No investment advice."
    )



    rules = f"""OUTPUT SPEC (STRICT — FOLLOW EXACTLY):

    1) BULLETS
    - Write up to three concise bullets.
    - Each bullet must start with "- " (dash + space).
    - EVERY factual statement (numbers, percentages, dates, “up/down”, specific claims)
        MUST end with one or more citation markers.
    - Citation marker format (STRICT):
        [S# pPAGE "SHORT QUOTE"]
        Examples:
            [S1 p7 "traffic rose 3% year-on-year in FY25"]
            [S2 p3 "growth in comparable store sales during H1"]

    RULES FOR CITATION MARKERS:
        - S# is the source index shown in CANDIDATES (1-based).
        - PAGE is the page number from the [S# pN] prefix in the context.
        - SHORT QUOTE:
            * EXACT, verbatim text copied from the underlying context for that S and page.
            * Must be 6–12 consecutive words.
            * No paraphrasing, no substitutions, no reordering.
        - Place the marker IMMEDIATELY after the sentence or clause it supports.
        - A bullet may contain multiple markers if using multiple claims.

    2) CITATIONS(JSON)
    After the bullets, output EXACTLY this line:
        CITATIONS(JSON)
    On the next line output ONLY a valid JSON array, e.g.:
        [
        {{ "bullet": 1, "S": 1, "page": 7, "quote": "traffic rose 3% year-on-year in FY25" }},
        {{ "bullet": 1, "S": 2, "page": 3, "quote": "growth in comparable store sales during H1" }}
        ]

    JSON RULES:
        - One object per citation marker used in the bullets.
        - "bullet" is 1-based bullet index.
        - "S" matches the S# from the marker.
        - "page" is the same page number as in the marker.
        - "quote" exactly matches the SHORT QUOTE used inside that marker.
        - JSON must be valid: no comments, no trailing commas.

    3) Sources
    After the JSON array, output a “Sources” section:
        Sources
        - <Exact title from CANDIDATES> — p.N[, p.M ...] — "ONE REPRESENTATIVE QUOTE"

    RULES:
        - The first line must be exactly: Sources
        - Then one bullet line per DISTINCT cited source.
        - Titles MUST match exactly what appears in CANDIDATES.
        - List ALL cited pages for that source, sorted ascending (e.g. "p.3, p.4, p.9").
        - The trailing quoted text must be ONE of the SHORT QUOTEs you used for that source.
        - All text must be copied exactly from context—no paraphrasing.

    GENERAL RULES (CRITICAL):
    - The most important Criteria for choosing sources is THE MOST RECENT, RELEVANT SOURCE.
    - YOU MUST, for every single dotpoint have the date of the given report Listed in month-yyyy (Jan-2025) at the start of the dotpoint. 
    - When answering with metrics, you MUST NOT quote multiple of the same/ similar metrics from multiple reports. You must only choose the most upto date metric, and quote in brackets the date when it was given. 
    - You MUST NOT use ANY information not found in the provided context.
    - If the context does not include sufficient information, write fewer bullets or none.
    - Every factual assertion must be grounded in a verbatim quote.
    - If you cannot find a valid 6–12 word quote, you may NOT make the claim.
    - You may skip bullets entirely if the source text is too thin.
    - Precision over breadth: fewer correct bullets > more speculative content.
    - Use Australian-English spelling and syntax pelase. 
    """




    system = base_persona + "\n\n" + rules + "\nCANDIDATES:\n" + candidates_block
    user = (
        f"User query: {user_query}\n\n"
        f"Context snippets (each prefixed with [S# pN]):\n{sources_text}\n\n"
        "Write the bullets, then the CITATIONS(JSON) array, then the final 'Sources' section now. "
        "Do not add anything else."
    )

    r = openai_manager.CLIENT.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        temperature=0.2,
    )
    out = (r.choices[0].message.content or "").strip()
    print(f"query_manager:llm_summarize_persona:DEBUG: llm_response: {out}")

    # FLOW: Split out bullets / CITATIONS(JSON) / Sources 
    lines = out.splitlines()
    bullets, sources_lines = [], []
    in_sources = False
    for ln in lines:
        if re.match(r"^\s*CITATIONS\(JSON\)\s*$", ln.strip(), flags=re.I):
            # stop collecting bullets here; JSON will be parsed separately
            break
        if re.match(r"^\s*sources\s*:?\s*$", ln.strip(), flags=re.I):
            in_sources = True
            continue
        if in_sources:
            if ln.strip():
                sources_lines.append(ln.strip())
        else:
            if ln.strip().startswith(("-", "*", "•")):
                bullets.append(ln.rstrip())

    bullets_md = "\n".join(bullets[:3]).rstrip()
    sources_md = "\n".join(sources_lines).strip()

    # FLOW: Format 
    # Parse the CITATIONS(JSON) array 
    m = re.search(r"CITATIONS\(JSON\)\s*(\[.*?\])", out, flags=re.S | re.I)
    citations_json = []
    if m:
        try:
            citations_json = json.loads(m.group(1))
        except Exception:
            citations_json = []

    # Build HTML with live, clickable [S# pN] anchors 
    bullets_html = _html_with_clickable_citations(bullets_md, sources_for_prompt)

    # Link 'Sources' titles to the first cited page for that source 
    link_map = {}
    for i, s in enumerate(sources_for_prompt, 1):
        # First cited page for S=i in the JSON, else the first page from pages list
        cited_pages = [int(c.get("page", 1)) for c in citations_json if int(c.get("S", 0)) == i]
        page = cited_pages[0] if cited_pages else int((s.get("pages") or [s.get("page") or 1])[0])
        href = _link_for_citation([s], 1, page, s.get("quote_hint", ""))
        if href:
            link_map[s["title"]] = {"url": href, "page": page}

    sources_html = ""
    if sources_md:
        sources_html = markdown_to_html("Sources\n" + sources_md, link_map=link_map)

    summary_html = bullets_html + (sources_html if sources_html else "")

    # Keep existing “references” extraction for compatibility
    references = []
    for ln in sources_lines:
        m2 = re.match(r"[-*•]\s*(.+?)\s*[—-]\s*p\.(.+)\s*$", ln, flags=re.I)
        if not m2:
            continue
        title = m2.group(1).strip()
        pages_str = m2.group(2).strip()
        pages = []
        for tok in re.split(r"[,\s]+", pages_str):
            tok = tok.strip().strip(",")
            if tok.isdigit():
                pages.append(int(tok))
        if pages:
            references.append({"title": title, "pages": pages})

    summary_md = bullets_md + ("\n\nSources\n" + sources_md if sources_md else "")

    return {
        "summary_md": summary_md,
        "summary_html": summary_html,
        "references": references,
        "citations": citations_json,
        "out": out,
    }

def prefer_pdf_path(p: Path | None) -> Path | None:
    """Swap .docx -> .pdf; leave others as-is."""
    if p is None:
        return None
    return p.with_suffix(".pdf") if p.suffix.lower() == ".docx" else p

def _find_ancestor_named(p: Path, name: str) -> Path | None:
    low = name.lower()
    for a in [p] + list(p.parents):
        if a.name.lower() == low:
            return a
    return None

def canonicalize_to_primary(abs_path: str | Path | None) -> Path | None:
    """Map any absolute path (even under ...Docx Retail copy) to the primary Docx Retail tree."""
    if not abs_path:
        return None
    try:
        p = Path(abs_path).resolve()
    except Exception:
        p = Path(str(abs_path))

    # Already under primary?
    try:
        p.relative_to(config.DOCX_RETAIL_PATH)
        return p
    except Exception:
        pass

    # Under the copy root?
    try:
        rel = p.relative_to(config.DOCX_RETAIL_COPY_PATH)
        return config.DOCX_RETAIL_PATH / rel
    except Exception:
        pass

    # Folder literally named "Docx Retail copy" somewhere up the tree?
    anc = _find_ancestor_named(p, "Docx Retail copy")
    if anc:
        rel = p.relative_to(anc)
        return config.DOCX_RETAIL_PATH / rel

    # Last-resort string replace
    s = str(p)
    if "Docx Retail copy" in s:
        q = Path(s.replace("Docx Retail copy", "Docx Retail"))
        try:
            q.relative_to(config.DOCX_RETAIL_PATH)
            return q
        except Exception:
            return None

    return None

def abs_path_to_media_docx_url(abs_path: str | Path | None, page: int | None = None) -> str | None:
    """
    Convert an absolute (possibly ...copy) path to a /media/docx URL served by FastAPI,
    preferring .pdf; append #page=N if given.
    """
    p = canonicalize_to_primary(abs_path)
    if p is None:
        return None
    p = prefer_pdf_path(p)
    if not p.exists():
        return None

    rel = p.relative_to(config.DOCX_RETAIL_PATH)
    url = "/media/docx/" + "/".join(quote(seg) for seg in rel.parts)
    if page:
        url += f"#page={page}"
    return url

def build_click_url_from_row(
    db_label: str,
    document_id: int,
    conn: sqlite3.Connection,
    default_page: int = 1
) -> str:
    """
    Prefer a direct /media/docx URL mapped from meta.absolute_path (or file_uri),
    adding #page=N. If mapping isn't possible, fall back to /view/{db}/{id}?page=N.

    Requires:
      - db()            -> sqlite connection
      - fetchone(...)   -> helper to run a single-row query
      - abs_path_to_media_docx_url(abs_path, page=None) -> maps absolute path
        under 'Docx Retail' tree to /media/docx/... and appends #page.
    """
    try:
        row = database_manager.fetchone(conn,
            f"SELECT meta, file_uri FROM {db_label}.document WHERE document_id=?",
            (document_id,)
        )
        if not row:
            # Fallback straight to viewer if row missing (should not happen ..... hopefully lol)
            return f"/view/{db_label}/{document_id}?page={int(default_page or 1)}"

        # Extract absolute path preference from meta; fall back to file_uri
        meta_raw = row["meta"] or "{}"
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, (str, bytes)) else (meta_raw or {})
        except Exception:
            meta = {}

        abs_path = (meta.get("absolute_path") or "").strip() \
                   or (meta.get("file_uri") or "").strip() \
                   or (row.get("file_uri") or "").strip()

        # Try to map to /media/docx ... and anchor to the page
        url = abs_path_to_media_docx_url(abs_path, page=int(default_page or 1)) if abs_path else None
        if url:
            return url

        # Last resort: id-based viewer
        return f"/view/{db_label}/{document_id}?page={int(default_page or 1)}"
    finally:
        conn.close()





# =========================================
# MAIN
# =========================================
def main(q, top_k, conn):
    print(f"query_manager:main:FLOW: entered overview with q='{q}' top_k={top_k}")
    top_k = 10


    # =========================================
    # STEP 0: Basic query processing
    # =========================================
    
    tokens = _parse_query(q)
    tickers = _guess_tickers(tokens)
    print(f"query_manager:main:DEBUG: tokens={tokens} tickers={tickers}")

    print(f"query_manager:main:FLOW: finish step 0")

    # =========================================
    # STEP 1A: Check case 1/2
    # =========================================

    out = classify_use_case(q)
    use_case = out['use_case']

    if use_case not in ['use_case_1', 'use_case_2']:
        print(f"query_manager:main:ERROR: use_case not valid! : debug classify_use_case output: {out}")
        return


    # =========================================
    # STEP 1B: Dispatch to case 1/2 workflow handlers
    # =========================================
    if use_case == "use_case_1":
        pool, tickers, extra_terms = handle_use_case_1(q, tokens, tickers, conn)
    if use_case == "use_case_2":
        pool, tickers, extra_terms = handle_use_case_2(q, tokens, out, conn)
    if not pool:
        print(f"query_manager:main:ERROR: No pool returned -> abort : pool: {pool}, tickers: {tickers}, extra_terms: {extra_terms}")
        return


    print(f"query_manager:main:FLOW: finish step 1")
    # =========================================
    # STEP 2: Fetch and rank docs that relate to the chosen company
    # =========================================
    
    # add on the abs path from documents table meta 
    pool = [dict(r) for r in pool]
    for r in pool:
        doc_id = r["document_id"]
        r["path_date"] = get_doc_path_date(conn, doc_id)
    from datetime import datetime
    # Deduplicate by document_id (keep first occurrence)
    seen = set()
    deduped_pool = []
    for r in pool:
        doc_id = r["document_id"]
        if doc_id not in seen:
            seen.add(doc_id)
            deduped_pool.append(r)

    pool = deduped_pool
    print(f"query_manager:main:DEBUG: [UC2] pool_size (deduped)={len(pool)}")
    ranked = pool

    if use_case == "use_case_2" and not tickers:
        print("query_manager:main:DEBUG: [RANK] use_case_2 + no tickers -> sorting by lowest total_hits then path_date DESC")

        ranked = sorted(
            pool,
            key=lambda r: (
                int(r.get("total_hits") or 0),
                -(r["path_date"].timestamp() if r.get("path_date") else 0)   # SAFE
            )
        )


    picked = ranked[:top_k]
    picked_sorted = sorted(picked, key=pubdate, reverse=True)
    print(
        f"query_manager:main:DEBUG: picked_docs={len(picked)} "
        f"ids={[int(r['document_id']) for r in picked]}"
    )


        # Build refs with derived fields
    refs: List[Dict[str, Any]] = []
    # We only know if alias_hits exists in DB; dynamic pool always has alias_hits key
    have_alias_col = database_manager._col_exists(conn, "company_term_count", "alias_hits")

    for r in picked_sorted:
        did = int(r["document_id"])

        title = ""
        published_at = ""
        source_url = ""
        source_path = ""

        try:
            doc_fields = database_manager.get_document_fields(conn, did)
        except Exception as e:
            print(f"query_manager:main:WARN: get_document_fields failed for document_id={did}: {e!r}")
            doc_fields = {}

        if doc_fields:
            title = doc_fields.get("title") or ""
            published_at = doc_fields.get("published_at") or ""
            source_url = doc_fields.get("source_url") or ""

            meta = doc_fields.get("meta") or {}
            source_path = (
                meta.get("absolute_path")
                or doc_fields.get("source_path")
                or ""
            )

        print(f"query_manager:main:DEBUG: doc_id={did} source_path={source_path!r}")

        ref = {
            "document_id": did,
            "title": title,
            "published_at": published_at,
            "source_url": source_url,
            "source_path": source_path,
            "company_id": int(r["company_id"]),
            "total_hits": int(r["total_hits"] or 0),
            "name_hits": int(r["name_hits"] or 0),
            "ticker_hits": int(r["ticker_hits"] or 0),
        }
        if "alias_hits" in r.keys():
            ref["alias_hits"] = int(r["alias_hits"] or 0)
        elif have_alias_col:
            ref["alias_hits"] = 0

        refs.append(ref)

    print(f"query_manager:main:DEBUG: refs_built={len(refs)}")

    print(f"query_manager:main:FLOW: finish step 2")
    # =========================================
    # STEP 3: Build context blocks & run LLM persona summary
    # =========================================
    context_blocks, sources_for_prompt = _build_context_blocks(conn, refs)
    print(
        "query_manager:main:DEBUG: context_blocks_nonempty="
        f"{sum(1 for b in context_blocks if b.strip())} / {len(context_blocks)}"
    )

    # Safety: if for some reason chunks are totally empty, just bail
    if not any(b.strip() for b in context_blocks):
        print("query_manager:main:ERROR: No non-empty context blocks after chunk fetch -> aborting.")
        return

    llm_out = llm_summarize_persona(
        conn,
        context_blocks=context_blocks,
        user_query=q,
        use_case="use_case_1",  # keep as-is unless you want to branch on use_case
        sources_for_prompt=sources_for_prompt,
    )

    # =========================================
    # STEP 4: Link refs chosen by the model
    # =========================================
    llm_refs = llm_out.get("references", [])
    linked_refs: List[Dict[str, Any]] = []
    for ref in llm_refs:
        ref_title_l = (ref.get("title") or "").lower()
        best = None
        for s in sources_for_prompt:
            st = (s["title"] or "").lower()
            if st in ref_title_l or ref_title_l in st:
                best = s
                break
        if best:
            page = (ref.get("pages") or [best.get("pages", [1])[0]])[0]
            url = (
                best.get("url_pdfjs")
                or best.get("url")
                or build_click_url_from_row(
                    best.get("db", "main"),
                    best["document_id"],
                    page,
                    conn,
                )
            )
            linked_refs.append(
                {
                    "title": best["title"],
                    "pages": ref.get("pages")
                    or best.get("pages")
                    or [page],
                    "url": url,
                    "db": best.get("db", "main"),
                    "document_id": best["document_id"],
                    "published_at": best.get("published_at"),
                }
            )

    print("query_manager:main:FLOW: finish step 3, creating final payload to return . . . ")

    # =========================================
    # STEP 5: Build final payload
    # =========================================
    summary_md = llm_out["summary_md"]
    citations = llm_out["citations"]  # e.g. [{bullet, S, page, quote}, ...]
    references = parse_sources_from_llm_output(llm_out["out"])

    # Persist to whatever QA log you have
    append_qa_output(
        question=q,
        summary_md=summary_md,
        citations=citations,
        references=references,
    )

    data = {
        "summary": summary_md,
        "summary_html": llm_out["summary_html"],
        "links": linked_refs,
        # NOTE: this array is what the frontend and worker treat as S1, S2, S3...
        "sources": sources_for_prompt,
        "inline_citations": citations,
    }
    return data
