from __future__ import annotations

import html
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

import query_manager
import database_manager
import config

# -------------------------------------------------
# APP SETUP
# -------------------------------------------------
app = FastAPI(title="CraigAI")

# CORS (relaxed so you can hit it from anywhere / JS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Optional: static files (if you have a ./static folder)
# Comment this out if you don't have a static/ directory
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except RuntimeError:
    # No static folder – ignore
    pass


# -------------------------------------------------
# SIMPLE HTML FRONTEND
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
            <span class="pill">Waiting for query…</span>
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
      // Summary
      if (data.summary_html) {
        summaryContentEl.innerHTML = data.summary_html;
      } else if (data.summary) {
        summaryContentEl.innerHTML = "<pre>" + escapeHtml(data.summary) + "</pre>";
      } else {
        summaryContentEl.innerHTML = "<em>No summary returned.</em>";
      }

      // Sources
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

      // Inline citations
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

      // Raw JSON
      rawJsonEl.textContent = JSON.stringify(data, null, 2);
    }

    async function runSearch(event) {
      event.preventDefault();

      const q = queryInput.value.trim();
      if (!q) return;

      statusEl.textContent = "Searching…";
      submitBtn.disabled = true;

      try {
        const params = new URLSearchParams({
          q,
          confirm: "false",
          top_k: "5"  // default
        });

        const response = await fetch(`/overview?${params.toString()}`);
        if (!response.ok) {
          const text = await response.text();
          throw new Error(`HTTP ${response.status}: ${text}`);
        }

        const data = await response.json();
        statusEl.textContent = "Done.";
        renderResult(data);
      } catch (err) {
        console.error(err);
        statusEl.textContent = "Error: " + (err?.message || String(err));
      } finally {
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


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """Default page with the search bar."""
    return HTML_INDEX


# -------------------------------------------------
# /overview JSON API
# -------------------------------------------------
@app.get("/overview")
def overview(
    q: str = Query(...),
    confirm: bool = Query(False),
    top_k: int = Query(5, ge=1, le=10)
):
    conn = database_manager.db(config.DB_PATH_MAIN)
    # database_manager.print_gen_doc_ids(conn)

    top_k = 20
    try:
        result = query_manager.main(q, top_k, conn)
    finally:
        conn.close()
    return result


# -------------------------------------------------
# LOCAL DEV ENTRYPOINT
# -------------------------------------------------
if __name__ == "__main__":
  import uvicorn

  uvicorn.run(app, host="127.0.0.1", port=8000)
  # uvicorn.run(app, host="0.0.0.0", port=8000)




