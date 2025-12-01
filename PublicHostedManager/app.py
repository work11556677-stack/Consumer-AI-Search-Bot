from __future__ import annotations

import threading
import uuid
from typing import Any, Dict

from flask import Flask, jsonify, request, Response

app = Flask(__name__)



jobs_lock = threading.Lock()
# job schema:
# {
#   "id": str,
#   "query": str,
#   "top_k": int,
#   "status": "pending" | "processing" | "done",
#   "result": dict | None
# }
jobs: Dict[str, Dict[str, Any]] = {}

ADMIN_API_KEY = "something_super_secrete_adfjdafhkjlkhethjlkj235770984175%$H^^GFS$^#$YSGHS^E$^HGASDFfadhfjahjlkh"

# -------------------------------------------------
# HTML FRONTEND
# -------------------------------------------------
HTML_INDEX = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Overview Search</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background-color: #0f172a;
      color: #e5e7eb;
    }
    body {
      margin: 0;
      padding: 0;
    }
    .page {
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 2rem;
    }
    .card {
      width: 100%;
      max-width: 1000px;
      background: #020617;
      border-radius: 16px;
      padding: 24px 28px;
      box-shadow: 0 18px 45px rgba(0,0,0,0.65);
      border: 1px solid #1e293b;
    }
    h1 {
      margin: 0 0 0.35rem 0;
      font-size: 1.5rem;
      letter-spacing: 0.03em;
    }
    .subtitle {
      margin-bottom: 1.5rem;
      font-size: 0.9rem;
      color: #9ca3af;
    }
    form {
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      margin-bottom: 1rem;
      align-items: center;
    }
    .field-group {
      flex: 1 1 260px;
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }
    label {
      font-size: 0.8rem;
      color: #9ca3af;
    }
    input[type="text"] {
      border-radius: 999px;
      border: 1px solid #1f2937;
      background-color: #020617;
      color: #e5e7eb;
      padding: 0.55rem 0.9rem;
      font-size: 0.9rem;
      outline: none;
    }
    input[type="text"]:focus {
      border-color: #4b5563;
      box-shadow: 0 0 0 1px #4b5563;
    }
    button {
      border: none;
      border-radius: 999px;
      padding: 0.65rem 1.4rem;
      font-size: 0.9rem;
      font-weight: 500;
      cursor: pointer;
      background: #e5e7eb;
      color: #020617;
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      white-space: nowrap;
    }
    button:disabled {
      opacity: 0.6;
      cursor: default;
    }
    .status {
      margin-bottom: 0.5rem;
      min-height: 1.25rem;
      font-size: 0.8rem;
      color: #9ca3af;
    }

    .results {
      border-radius: 12px;
      background: #020617;
      border: 1px solid #111827;
      padding: 0.9rem 1rem;
      font-size: 0.85rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }
    .results-section {
      border-radius: 8px;
      padding: 0.5rem 0.75rem;
      background: rgba(15,23,42,0.8);
      border: 1px solid #1f2937;
    }
    .results-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #9ca3af;
      margin-bottom: 0.35rem;
    }
    .results-header span {
      font-weight: 600;
    }
    .summary-content ul {
      margin: 0.25rem 0 0.25rem 1.1rem;
      padding: 0;
    }
    .summary-content li {
      margin-bottom: 0.2rem;
    }
    .pill {
      border-radius: 999px;
      padding: 0.15rem 0.6rem;
      border: 1px solid #374151;
      font-size: 0.7rem;
      color: #9ca3af;
    }
    .source-item, .citation-item {
      margin-bottom: 0.15rem;
      line-height: 1.4;
    }
    .source-title {
      font-weight: 500;
      color: #e5e7eb;
    }
    .source-meta {
      font-size: 0.75rem;
      color: #9ca3af;
    }
    .raw-json-toggle {
      font-size: 0.75rem;
      color: #9ca3af;
      cursor: pointer;
      text-decoration: underline;
      margin-top: 0.25rem;
    }
    .raw-json {
      margin-top: 0.3rem;
      max-height: 260px;
      overflow: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.75rem;
      line-height: 1.35;
      background: #020617;
      border-radius: 8px;
      padding: 0.5rem 0.75rem;
      border: 1px solid #111827;
      white-space: pre;
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <h1>Overview Search</h1>
      <div class="subtitle">
        Ask a question!
      </div>
      <form id="search-form">
        <div class="field-group">
          <label for="query">Question</label>
          <input id="query" type="text" placeholder="e.g. JBH outlook on gross margins" required />
        </div>
        <div>
          <label style="visibility:hidden;">Search</label>
          <button type="submit" id="submit-btn">
            Run search
          </button>
        </div>
      </form>
      <div id="status" class="status"></div>

      <div id="results" class="results">
        <div class="results-section">
          <div class="results-header">
            <span>Summary</span>
            <span class="pill" id="summary-pill">Waiting for query…</span>
          </div>
          <div id="summary-content" class="summary-content">
            <em>No results yet.</em>
          </div>
        </div>

        <div class="results-section">
          <div class="results-header">
            <span>Sources</span>
            <span class="pill" id="sources-count">0 docs</span>
          </div>
          <div id="sources-list">
            <em>No sources yet.</em>
          </div>
        </div>

        <div class="results-section">
          <div class="results-header">
            <span>Inline citations</span>
            <span class="pill" id="citations-count">0 refs</span>
          </div>
          <div id="citations-list">
            <em>No inline citations yet.</em>
          </div>
        </div>

        <div class="results-section">
          <div class="results-header">
            <span>Raw JSON</span>
          </div>
          <div class="raw-json-toggle" id="json-toggle">Show raw payload</div>
          <pre id="raw-json" class="raw-json" style="display:none;">{}</pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    const form = document.getElementById("search-form");
    const queryInput = document.getElementById("query");
    const statusEl = document.getElementById("status");
    const submitBtn = document.getElementById("submit-btn");

    const summaryContentEl = document.getElementById("summary-content");
    const summaryPillEl = document.getElementById("summary-pill");
    const sourcesListEl = document.getElementById("sources-list");
    const sourcesCountEl = document.getElementById("sources-count");
    const citationsListEl = document.getElementById("citations-list");
    const citationsCountEl = document.getElementById("citations-count");
    const rawJsonEl = document.getElementById("raw-json");
    const jsonToggleEl = document.getElementById("json-toggle");

    function escapeHtml(str) {
      return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    function renderResult(data) {
      if (data.summary_html) {
        summaryContentEl.innerHTML = data.summary_html;
      } else if (data.summary) {
        summaryContentEl.innerHTML = "<pre>" + escapeHtml(data.summary) + "</pre>";
      } else {
        summaryContentEl.innerHTML = "<em>No summary returned.</em>";
      }

      const sources = Array.isArray(data.sources) ? data.sources : [];
      sourcesCountEl.textContent = sources.length + (sources.length === 1 ? " doc" : " docs");

      if (sources.length === 0) {
        sourcesListEl.innerHTML = "<em>No sources.</em>";
      } else {
        sourcesListEl.innerHTML = sources.map((src, idx) => {
          const sLabel = "S" + (idx + 1);
          const pages = Array.isArray(src.pages) ? src.pages.join(", ") : "";
          const title = src.title || "(untitled)";
          const docId = src.document_id != null ? src.document_id : "?";
          return (
            '<div class="source-item">' +
              '<div class="source-title">' + sLabel + " – " + escapeHtml(title) + "</div>" +
              '<div class="source-meta">document_id ' + docId + " · pages " + escapeHtml(pages) + "</div>" +
            "</div>"
          );
        }).join("");
      }

      const citations = Array.isArray(data.inline_citations) ? data.inline_citations : [];
      citationsCountEl.textContent = citations.length + (citations.length === 1 ? " ref" : " refs");

      if (citations.length === 0) {
        citationsListEl.innerHTML = "<em>No inline citations.</em>";
      } else {
        citationsListEl.innerHTML = citations.map((c) => {
          const bullet = c.bullet ?? "?";
          const s = c.S != null ? "S" + c.S : "?";
          const page = c.page != null ? "p" + c.page : "?";
          const quote = c.quote ? ' – “' + escapeHtml(c.quote) + '”' : "";
          return (
            '<div class="citation-item">' +
              "• Bullet " + bullet + " → " + s + " " + page + quote +
            "</div>"
          );
        }).join("");
      }

      rawJsonEl.textContent = JSON.stringify(data, null, 2);
    }

    async function pollJob(jobId) {
      summaryPillEl.textContent = "Waiting for backend…";
      while (true) {
        const resp = await fetch(`/api/job/${jobId}`);
        if (!resp.ok) {
          const text = await resp.text();
          statusEl.textContent = "Error: " + text;
          break;
        }
        const job = await resp.json();

        if (job.status === "done") {
          statusEl.textContent = "Done.";
          summaryPillEl.textContent = "Completed";
          renderResult(job.result || {});
          break;
        } else if (job.status === "pending" || job.status === "processing") {
          statusEl.textContent = "Processing on backend…";
          await new Promise(r => setTimeout(r, 2000));
        } else {
          statusEl.textContent = "Error: unexpected status " + job.status;
          break;
        }
      }
    }

    async function runSearch(event) {
      event.preventDefault();

      const q = queryInput.value.trim();
      if (!q) return;

      statusEl.textContent = "Submitting job…";
      submitBtn.disabled = true;
      summaryPillEl.textContent = "Queued";

      try {
        const resp = await fetch("/api/submit", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ q, top_k: 5 })
        });

        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(`HTTP ${resp.status}: ${text}`);
        }

        const data = await resp.json();
        const jobId = data.job_id;
        if (!jobId) {
          throw new Error("No job_id returned");
        }

        statusEl.textContent = "Job submitted. Waiting for result…";
        pollJob(jobId);
      } catch (err) {
        console.error(err);
        statusEl.textContent = "Error: " + (err?.message || String(err));
        submitBtn.disabled = false;
      }
    }

    form.addEventListener("submit", runSearch);

    jsonToggleEl.addEventListener("click", () => {
      const isHidden = rawJsonEl.style.display === "none";
      rawJsonEl.style.display = isHidden ? "block" : "none";
      jsonToggleEl.textContent = isHidden ? "Hide raw payload" : "Show raw payload";
    });
  </script>
</body>
</html>
"""

# -------------------------------------------------
# ROUTES
# -------------------------------------------------

@app.route("/", methods=["GET"])
def index() -> Response:
    return Response(HTML_INDEX, mimetype="text/html")


@app.route("/api/submit", methods=["POST"])
def submit():
    data = request.get_json(silent=True) or {}
    q = (data.get("q") or "").strip()
    top_k = data.get("top_k") or 5

    if not q:
        return jsonify({"error": "Query cannot be empty"}), 400

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "query": q,
        "top_k": int(top_k),
        "status": "pending",
        "result": None,
    }

    with jobs_lock:
        jobs[job_id] = job

    return jsonify({"job_id": job_id})


@app.route("/api/job/<job_id>", methods=["GET"])
def get_job(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        return jsonify({"detail": "Job not found"}), 404

    return jsonify(
        {
            "id": job["id"],
            "status": job["status"],
            "result": job["result"] if job["status"] == "done" else None,
        }
    )


@app.route("/api/admin/next_job", methods=["GET"])
def admin_next_job():
    api_key = request.args.get("api_key")
    if api_key != ADMIN_API_KEY:
        return jsonify({"detail": "Invalid API key"}), 401

    with jobs_lock:
        for job in jobs.values():
            if job["status"] == "pending":
                job["status"] = "processing"
                return jsonify(
                    {
                        "id": job["id"],
                        "query": job["query"],
                        "top_k": job["top_k"],
                        "status": job["status"],
                    }
                )

    return jsonify({"id": None, "status": "idle"})


@app.route("/api/admin/job/<job_id>/complete", methods=["POST"])
def admin_complete_job(job_id: str):
    api_key = request.args.get("api_key")
    if api_key != ADMIN_API_KEY:
        return jsonify({"detail": "Invalid API key"}), 401

    data = request.get_json(silent=True) or {}
    result = data.get("result")
    if result is None:
        return jsonify({"detail": "Missing 'result'"}), 400

    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"detail": "Job not found"}), 404

        job["status"] = "done"
        job["result"] = result

    return jsonify({"ok": True})