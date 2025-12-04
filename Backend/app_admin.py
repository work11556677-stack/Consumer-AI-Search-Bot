from __future__ import annotations

import time
from typing import Any, Dict

import requests
from requests.auth import HTTPBasicAuth 
import query_manager
import database_manager
import config
from pathlib import Path
import os 


PA_BASE_URL = "https://RM1234567890.pythonanywhere.com"
# PA_BASE_URL = "http://127.0.0.1:5000/"
LAST_MAIN_QUERY = ""


ADMIN_API_KEY = "something_super_secrete_adfjdafhkjlkhethjlkj235770984175%$H^^GFS$^#$YSGHS^E$^HGASDFfadhfjahjlkh"
BASIC_AUTH_USERNAME = "consumerteam"
BASIC_AUTH_PASSWORD = "isthebest"

AUTH = HTTPBasicAuth(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD)

def fetch_next_job() -> Dict[str, Any] | None:
    """Ask the broker (PythonAnywhere) for the next pending job."""
    resp = requests.get(
        f"{PA_BASE_URL}/api/admin/next_job",
        params={"api_key": ADMIN_API_KEY},
        auth=AUTH,
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    job_id = data.get("id")
    if not job_id:
        return None
    return data


def send_job_result(job_id: str, result: Dict[str, Any]) -> None:
    """Send computed result back to the broker."""
    resp = requests.post(
        f"{PA_BASE_URL}/api/admin/job/{job_id}/complete",
        params={"api_key": ADMIN_API_KEY},
        json={"result": result},
        auth=AUTH,
        timeout=30,
    )
    resp.raise_for_status()




def process_job(job: Dict[str, Any]) -> None:
    job_id = job["id"]
    q = job["query"]
    reformulate = job['reformulate']
    top_k = job.get("top_k", 5)
    job_type = job.get("job_type", "search")

    print(f"[worker] Processing job {job_id!r} – query={q!r}, top_k={top_k}")

    conn = database_manager.db(config.DB_PATH_MAIN)
    try:
        if job_type == "search": 
            global LAST_MAIN_QUERY
            LAST_MAIN_QUERY = q
            print(f"app_admin:process_job:DEBUG: updated last_main_query with : {LAST_MAIN_QUERY}")
            result = query_manager.main(q, top_k, conn, reformulate)
            sources = result.get("sources") or []
            citations = result.get("inline_citations") or []

            used_S = set()
            for c in citations:
                s_val = c.get("S")
                if isinstance(s_val, int):
                    used_S.add(s_val)

            for s in sorted(used_S):
                idx = s - 1
                if idx < 0 or idx >= len(sources):
                    print("1")
                    continue

                src = sources[idx]
                doc_id = src.get("document_id")
                source_path = src.get("source_path") or ""

                if not doc_id or not source_path:
                    continue

                pdf_path = source_path_to_pdf_path(source_path)
                if not pdf_path:
                    continue

                try:
                    upload_pdf(job_id, str(doc_id), pdf_path)
                    src["url"] = f"{PA_BASE_URL}/pdf/{job_id}/{doc_id}"
                    print(f"[worker] Uploaded PDF for doc_id={doc_id}: {pdf_path}")
                except Exception as e:
                    print(f"[worker] Failed to upload PDF for doc_id={doc_id}: {e!r}")
        
        
        elif job_type == "expand_bullet":
            bullet_text = job.get("query") or ""
            doc_id = job.get("doc_id")
            if not bullet_text or doc_id is None:
                raise ValueError("expand_bullet job missing bullet_text or doc_id")
            print(f"[worker]  expand_bullet: doc_id={doc_id}, bullet={bullet_text!r}")
            result = query_manager.expand_bullet(conn, int(doc_id), bullet_text, LAST_MAIN_QUERY)

        else:
            raise ValueError(f"Unknown job_type {job_type!r}")

    finally:
        conn.close()

    print(f"[worker] Finished job {job_id!r}, sending result back…")
    send_job_result(job_id, result)
    print(f"[worker] Result for job {job_id!r} sent.")



def source_path_to_pdf_path(source_path: str) -> str | None:
    """
    Convert a DOCX source_path (from DB/meta) into a PDF path on disk.

    Logic:
      - Find the 'Docx Retail copy' segment (case-insensitive).
      - Keep everything *after* that segment as a relative path.
      - Rebuild: config.HOME_DIR / relative.with_suffix('.pdf').

    So:
      C:\\...\\V4\\Docx Retail copy\\GEN\\file.docx
    becomes:
      <config.HOME_DIR>\\GEN\\file.pdf
    """
    if not source_path:
        return None

    p = Path(source_path)
    parts = list(p.parts)

    # case-insensitive search for 'Docx Retail copy'
    anchor_idx = None
    for i, part in enumerate(parts):
        if part.lower() == "docx retail copy".lower():
            anchor_idx = i
            break

    if anchor_idx is not None:
        # everything AFTER 'Docx Retail copy' → e.g. GEN/file.docx
        rel = Path(*parts[anchor_idx + 1 :])
        pdf_rel = rel.with_suffix(".pdf")
        base = Path(config.DOCX_RETAIL_PATH)
        pdf_path = base / pdf_rel
    else:
        # Fallback: just swap extension in-place if we can't find the anchor
        pdf_path = p.with_suffix(".pdf")

    return str(pdf_path)


def upload_pdf(job_id: str, doc_id: str, pdf_path: str) -> None:
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(pdf_path)

    with open(pdf_path, "rb") as f:
        files = {"file": (os.path.basename(pdf_path), f, "application/pdf")}
        resp = requests.post(
            f"{PA_BASE_URL}/api/admin/job/{job_id}/upload_pdf",
            params={"api_key": ADMIN_API_KEY, "doc_id": doc_id},
            files=files,
            auth=AUTH,  
            timeout=60,
        )
    resp.raise_for_status()

def main_loop():
    print("[worker] Starting admin worker loop…")
    while True:
        # try:
        #     job = fetch_next_job()
        #     if not job:
        #         time.sleep(2.0)
        #         continue

        #     process_job(job)

        # except KeyboardInterrupt:
        #     print("[worker] Stopping due to keyboard interrupt.")
        #     break
        # except Exception as e:
        #     print(f"[worker] Error: {e!r}")
        #     time.sleep(5.0)

        job = fetch_next_job()
        if not job:
            time.sleep(2.0)
            continue

        process_job(job)




if __name__ == "__main__":
    main_loop()
