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
  <title>Ask CrAIg</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --color-navy: #00205C;      /* rgb(0,32,92) */
      --color-teal: #62CBC9;      /* rgb(98,203,201) */
      --color-warm-grey: #C5B8AC; /* rgb(197,184,172) */
      --color-bg: #00173f;
      --color-card: #0b1220;
      --color-card-soft: #0f172a;
      --color-border: rgba(148,163,184,0.2);
      --color-text: #F9FAFB;
      --color-muted: #9CA3AF;

      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background-color: var(--color-bg);
      color: var(--color-text);
    }
    * {
      box-sizing: border-box;
    }
    body {
      margin: 0;
      padding: 0;
      /* Teal outer, fading into white around the centre where the card sits */
      background: radial-gradient(
          circle at center,
          #ffffff 0%,
          #ffffff 35%,
          rgba(98,203,201,0.45) 70%,
          #62CBC9 100%
        );
      color: #111827;
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
      background: linear-gradient(
        135deg,
        rgba(0,32,92,0.96),
        rgba(11,18,32,0.98)
      );
      border-radius: 20px;
      padding: 24px 28px;
      box-shadow:
        0 25px 60px rgba(0,0,0,0.8),
        0 0 0 1px rgba(148,163,184,0.15);
      border: 1px solid rgba(98,203,201,0.35);
      backdrop-filter: blur(18px);
    }
    h1 {
      margin: 0 0 0.35rem 0;
      font-size: 1.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      display: flex;
      align-items: baseline;
      gap: 0.4rem;
    }
    h1 span.logo-pill {
      padding: 0.15rem 0.55rem;
      border-radius: 999px;
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.15em;
      border: 1px solid rgba(98,203,201,0.6);
      color: var(--color-teal);
      background: rgba(15,23,42,0.8);
    }
    .subtitle {
      margin-bottom: 1.5rem;
      font-size: 0.92rem;
      color: var(--color-muted);
    }
    form {
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      margin-bottom: 1rem;
      align-items: flex-end;
    }
    .field-group {
      flex: 1 1 260px;
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }
    label {
      font-size: 0.8rem;
      color: var(--color-warm-grey);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    input[type="text"] {
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.45);
      background-color: rgba(15,23,42,0.95);
      color: var(--color-text);
      padding: 0.6rem 1rem;
      font-size: 0.9rem;
      outline: none;
      transition: border-color 0.15s ease, box-shadow 0.15s ease, background-color 0.15s ease;
    }
    input[type="text"]::placeholder {
      color: rgba(148,163,184,0.7);
    }
    input[type="text"]:focus {
      border-color: var(--color-teal);
      box-shadow: 0 0 0 1px rgba(98,203,201,0.8);
      background-color: #020617;
    }
    button {
      border: none;
      border-radius: 999px;
      padding: 0.65rem 1.5rem;
      font-size: 0.9rem;
      font-weight: 500;
      cursor: pointer;
      background: linear-gradient(135deg, var(--color-teal), #8ff0eb);
      color: #00111a;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      white-space: nowrap;
      box-shadow: 0 12px 25px rgba(0,0,0,0.55);
      transition: transform 0.08s ease, box-shadow 0.08s ease, filter 0.08s ease;
    }
    button::after {
      content: "↵";
      font-size: 0.9rem;
      opacity: 0.8;
    }
    button:hover:not(:disabled) {
      transform: translateY(-1px);
      box-shadow: 0 16px 35px rgba(0,0,0,0.7);
      filter: brightness(1.03);
    }
    button:active:not(:disabled) {
      transform: translateY(0);
      box-shadow: 0 10px 20px rgba(0,0,0,0.6);
      filter: brightness(0.98);
    }
    button:disabled {
      opacity: 0.6;
      cursor: default;
      box-shadow: none;
    }
    .status {
      margin-bottom: 0.5rem;
      min-height: 1.25rem;
      font-size: 0.8rem;
      color: var(--color-muted);
    }

    .results {
      border-radius: 16px;
      background: radial-gradient(circle at top left, rgba(98,203,201,0.06), transparent 55%),
                  radial-gradient(circle at bottom right, rgba(197,184,172,0.06), transparent 55%),
                  rgba(15,23,42,0.96);
      border: 1px solid var(--color-border);
      padding: 0.95rem 1rem;
      font-size: 0.85rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }
    .results-section {
      border-radius: 12px;
      padding: 0.6rem 0.8rem;
      background: rgba(15,23,42,0.92);
      border: 1px solid rgba(148,163,184,0.25);
    }
    .results-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--color-muted);
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
      margin-bottom: 0.25rem;
      line-height: 1.45;
    }
    .pill {
      border-radius: 999px;
      padding: 0.15rem 0.6rem;
      border: 1px solid rgba(148,163,184,0.4);
      font-size: 0.7rem;
      color: var(--color-muted);
      background: rgba(15,23,42,0.9);
    }
    .source-item, .citation-item {
      margin-bottom: 0.2rem;
      line-height: 1.45;
    }
    .source-title {
      font-weight: 500;
      color: var(--color-text);
    }
    .source-meta {
      font-size: 0.75rem;
      color: var(--color-muted);
    }
    .source-meta a {
      color: var(--color-teal);
      text-decoration: none;
    }
    .source-meta a:hover {
      text-decoration: underline;
    }
    .raw-json-toggle {
      font-size: 0.75rem;
      color: var(--color-muted);
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
      border: 1px solid rgba(55,65,81,0.9);
      white-space: pre;
    }

    /* Inline citation pill in the summary text */
    .citation-pill {
      display: inline-block;
      margin-left: 0.45rem;
      margin-top: 0.1rem;
      padding: 0.12rem 0.6rem;
      border-radius: 999px;
      border: 1px solid rgba(98,203,201,0.7);
      background: rgba(0,32,92,0.9);
      font-size: 0.75rem;
      color: var(--color-teal);
      white-space: nowrap;
    }
    .citation-pill a {
      color: inherit;
      text-decoration: none;
    }
    .citation-pill a:hover {
      text-decoration: underline;
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <h1>
        CrAIg
        <span class="logo-pill">MST Research Copilot</span>
      </h1>
      <div class="subtitle">
        Natural language in. CrAIg fetches the right reports, pulls the evidence, and shows exactly where it came from.
      </div>
      <form id="search-form">
        <div class="field-group">
          <label for="query">Question</label>
          <input id="query" type="text" placeholder="e.g. BRG: FY26e sales growth drivers and regional mix" required />
        </div>
        <div>
          <label style="visibility:hidden;">Search</label>
          <button type="submit" id="submit-btn">
            Ask CrAIg
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
      const sources = Array.isArray(data.sources) ? data.sources : [];
      const citations = Array.isArray(data.inline_citations) ? data.inline_citations : [];
      const summary = data.summary || "";

      // ===== 1) Build summary HTML ourselves from `summary` + citations =====
      if (summary) {
        const lines = summary.split("\\n").filter(line => line.trim().length > 0);
        const itemsHtml = lines.map((line, idx) => {
          const bulletNum = idx + 1;

          let text = line.replace(/^\\s*-\\s*/, "");
          text = text.replace(/\\[S\\d+\\s+p\\d+[^\\]]*\\]/g, "");
          text = text.trim();

          const citsForBullet = citations.filter(c => c.bullet === bulletNum);

          const pillsHtml = citsForBullet.map(c => {
            const sVal = c.S;
            const pageNum = c.page;
            const quoteText = c.quote || "";

            let href = null;
            if (Number.isInteger(sVal) && sVal >= 1 && sVal <= sources.length) {
              const src = sources[sVal - 1];
              if (src && src.url) {
                href = src.url;
                const params = [];
                if (pageNum != null) params.push("page=" + encodeURIComponent(pageNum));
                if (quoteText) params.push("quote=" + encodeURIComponent(quoteText));
                if (params.length > 0) {
                  href += (href.includes("?") ? "&" : "?") + params.join("&");
                }
              }
            }

            const safeQuote = escapeHtml(quoteText);
            const labelParts = [];
            if (safeQuote) {
              labelParts.push("“" + safeQuote + "”");
            }
            if (sVal != null) {
              let refLabel = "S" + sVal;
              if (pageNum != null) refLabel += ", p" + pageNum;
              labelParts.push(refLabel);
            }
            const label = labelParts.join(" — ");

            if (href) {
              return ' <span class="citation-pill"><a href="' + href + '" target="_blank" rel="noopener noreferrer">' + label + '</a></span>';
            } else {
              return ' <span class="citation-pill">' + label + '</span>';
            }
          }).join("");

          return "<li>" + escapeHtml(text) + pillsHtml + "</li>";
        }).join("");

        summaryContentEl.innerHTML = "<ul>" + itemsHtml + "</ul>";
      } else {
        summaryContentEl.innerHTML = "<em>No summary returned.</em>";
      }

      // ===== 2) SOURCES section =====
      sourcesCountEl.textContent = sources.length + (sources.length === 1 ? " doc" : " docs");

      if (sources.length === 0) {
        sourcesListEl.innerHTML = "<em>No sources.</em>";
      } else {
        sourcesListEl.innerHTML = sources.map((src, idx) => {
          const sLabel = "S" + (idx + 1);
          const pages = Array.isArray(src.pages) ? src.pages.join(", ") : "";
          const title = src.title || "(untitled)";
          const docId = src.document_id != null ? src.document_id : "?";
          const url = src.url;

          const linkHtml = url
            ? ' · <a href="' + url + '" target="_blank" rel="noopener noreferrer">Open PDF</a>'
            : "";

          return (
            '<div class="source-item">' +
              '<div class="source-title">' + sLabel + " – " + escapeHtml(title) + "</div>" +
              '<div class="source-meta">document_id ' + docId +
                " · pages " + escapeHtml(pages) +
                linkHtml +
              "</div>" +
            "</div>"
          );
        }).join("");
      }

      // ===== 3) INLINE CITATIONS list (sidebar) =====
      citationsCountEl.textContent = citations.length + (citations.length === 1 ? " ref" : " refs");

      if (citations.length === 0) {
        citationsListEl.innerHTML = "<em>No inline citations yet.</em>";
      } else {
        citationsListEl.innerHTML = citations.map((c) => {
          const bullet = c.bullet ?? "?";
          const sIndex = (typeof c.S === "number") ? c.S - 1 : null;
          const pageNum = c.page;
          const quoteText = c.quote || "";

          const sLabel = c.S != null ? "S" + c.S : "?";
          const pageLabel = pageNum != null ? "p" + pageNum : "?";
          const text =
            "• Bullet " + bullet + " → " + sLabel + " " + pageLabel +
            (quoteText ? ' – “' + escapeHtml(quoteText) + '”' : "");

          let href = null;
          if (sIndex != null && sIndex >= 0 && sIndex < sources.length && sources[sIndex].url) {
            href = sources[sIndex].url;
            const params = [];
            if (pageNum != null) params.push("page=" + encodeURIComponent(pageNum));
            if (quoteText) params.push("quote=" + encodeURIComponent(quoteText));
            if (params.length > 0) {
              href += (href.includes("?") ? "&" : "?") + params.join("&");
            }
          }

          if (href) {
            return (
              '<div class="citation-item">' +
                '<a href="' + href + '" target="_blank" rel="noopener noreferrer">' +
                  text +
                '</a>' +
              '</div>'
            );
          } else {
            return '<div class="citation-item">' + text + '</div>';
          }
        }).join("");
      }

      // ===== 4) Raw JSON debug =====
      rawJsonEl.textContent = JSON.stringify(data, null, 2);
    }

    async function pollJob(jobId) {
      summaryPillEl.textContent = "Waiting for backend…";
      while (true) {
        const resp = await fetch("/api/job/" + jobId);
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
          submitBtn.disabled = false;
          break;
        } else if (job.status === "pending" || job.status === "processing") {
          statusEl.textContent = "Processing on backend…";
          await new Promise(r => setTimeout(r, 2000));
        } else {
          statusEl.textContent = "Error: unexpected status " + job.status;
          submitBtn.disabled = false;
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
          body: JSON.stringify({ q: q, top_k: 5 })
        });

        if (!resp.ok) {
          const text = await resp.text();
          throw new Error("HTTP " + resp.status + ": " + text);
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
        statusEl.textContent = "Error: " + (err && err.message ? err.message : String(err));
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