# admin_worker.py
from __future__ import annotations

import time
from typing import Any, Dict

import requests
from requests.auth import HTTPBasicAuth 
import query_manager
import database_manager
import config

# PythonAnywhere URL
PA_BASE_URL = "https://RM1234567890.pythonanywhere.com"


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
    top_k = job.get("top_k", 5)

    print(f"[worker] Processing job {job_id!r} – query={q!r}, top_k={top_k}")

    conn = database_manager.db(config.DB_PATH_MAIN)
    try:
        # You had this already:
        # result = query_manager.main(q, top_k, conn)
        # (you also hard-set top_k=20 inside main; that's fine)
        result = query_manager.main(q, top_k, conn)
    finally:
        conn.close()

    print(f"[worker] Finished job {job_id!r}, sending result back…")
    send_job_result(job_id, result)
    print(f"[worker] Result for job {job_id!r} sent.")


def main_loop():
    print("[worker] Starting admin worker loop…")
    while True:
        try:
            job = fetch_next_job()
            if not job:
                # no work right now
                time.sleep(2.0)
                continue

            process_job(job)

        except KeyboardInterrupt:
            print("[worker] Stopping due to keyboard interrupt.")
            break
        except Exception as e:
            print(f"[worker] Error: {e!r}")
            # backoff a bit before retrying
            time.sleep(5.0)


if __name__ == "__main__":
    main_loop()
