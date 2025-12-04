from __future__ import annotations

import threading
import uuid
from typing import Any, Dict
import os
from flask import send_file
from werkzeug.utils import secure_filename
from flask import Flask, jsonify, request, Response, send_file, redirect

app = Flask(__name__)


PDF_TMP_DIR = "/tmp/craigai_pdfs"
os.makedirs(PDF_TMP_DIR, exist_ok=True)


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
      --color-bg: #00205C;
      --color-text-dark: #111827;
      --color-muted: #6B7280;

      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background-color: var(--color-bg);
      color: var(--color-text-dark);
    }
    * {
      box-sizing: border-box;
    }
    body {
      margin: 0;
      padding: 0;
      /* Teal outer, fading to white near the centre (under the card) */
      background: radial-gradient(
        circle at center,
        #ffffff 0%,
        #ffffff 35%,
        rgba(98,203,201,0.45) 70%,
        #62CBC9 100%
      );
      color: var(--color-text-dark);
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
      background: #ffffff;
      border-radius: 20px;
      padding: 24px 28px;
      box-shadow:
        0 18px 40px rgba(0,0,0,0.30),
        0 0 0 1px rgba(15,23,42,0.05);
      border: 1px solid rgba(148,163,184,0.4);
      color: var(--color-text-dark);
    }
    h1 {
      margin: 0 0 0.35rem 0;
      font-size: 1.7rem;
      letter-spacing: 0.04em;
      text-transform: none;
      display: flex;
      align-items: baseline;
      gap: 0.4rem;
    }
    h1 span.logo-pill {
      padding: 0.15rem 0.55rem;
      border-radius: 999px;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      border: 1px solid var(--color-teal);
      color: #111827;
      background: rgba(98,203,201,0.12);
      white-space: nowrap;
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
      color: var(--color-muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    input[type="text"] {
      border-radius: 12px;
      border: 2px solid var(--color-teal);
      background-color: #ffffff;
      color: var(--color-text-dark);
      padding: 0.6rem 0.8rem;
      font-size: 0.9rem;
      outline: none;
      transition: border-color 0.15s ease, box-shadow 0.15s ease;
    }
    input[type="text"]::placeholder {
      color: #9CA3AF;
    }
    input[type="text"]:focus {
      border-color: var(--color-teal);
      box-shadow: 0 0 0 2px rgba(98,203,201,0.35);
    }
    button {
      border: none;
      border-radius: 999px;
      padding: 0.65rem 1.6rem;
      font-size: 0.9rem;
      font-weight: 500;
      cursor: pointer;
      background: var(--color-teal);
      color: #111827;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      white-space: nowrap;
      box-shadow: 0 10px 25px rgba(0,0,0,0.25);
      transition: transform 0.08s ease, box-shadow 0.08s ease, filter 0.08s ease;
    }
    button::after {
      content: "↵";
      font-size: 0.9rem;
      opacity: 0.9;
    }
    button:hover:not(:disabled) {
      transform: translateY(-1px);
      box-shadow: 0 14px 30px rgba(0,0,0,0.3);
      filter: brightness(1.03);
    }
    button:active:not(:disabled) {
      transform: translateY(0);
      box-shadow: 0 8px 18px rgba(0,0,0,0.25);
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

    /* Results container – just a column of boxes */
    .results {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      margin-top: 0.5rem;
    }
    .results-section {
      border-radius: 14px;
      padding: 0.7rem 0.85rem;
      background: #ffffff;
      border: 2px solid var(--color-teal);
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
      gap: 0.5rem;
    }
    .results-header span {
      font-weight: 600;
    }
    .results-header-main {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      flex: 1 1 auto;
      min-width: 0;
    }
    .summary-content ul {
      margin: 0.25rem 0 0.25rem 1.1rem;
      padding: 0;
    }
    .summary-content li {
      margin-bottom: 0.25rem;
      line-height: 1.45;
      color: var(--color-text-dark);
    }
    .pill {
      border-radius: 999px;
      padding: 0.2rem 0.7rem;
      border: 1px solid rgba(148,163,184,0.5);
      font-size: 0.7rem;
      color: var(--color-muted);
      background: #F9FAFB;
    }
    .source-item, .citation-item {
      margin-bottom: 0.2rem;
      line-height: 1.45;
      color: var(--color-text-dark);
    }
    .source-title {
      font-weight: 500;
    }
    .source-meta {
      font-size: 0.75rem;
      color: var(--color-muted);
    }
    .source-meta a {
      color: #111827;
      text-decoration: underline;
      text-decoration-color: var(--color-teal);
      text-underline-offset: 2px;
    }
    .source-meta a:hover {
      text-decoration-thickness: 2px;
    }
    .raw-json {
      margin-top: 0.3rem;
      max-height: 260px;
      overflow: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.75rem;
      line-height: 1.35;
      background: #F9FAFB;
      border-radius: 8px;
      padding: 0.5rem 0.75rem;
      border: 1px solid rgba(148,163,184,0.6);
      white-space: pre;
      color: #111827;
    }

    /* Inline citation pill in the summary text */
    .citation-pill {
      display: inline-block;
      margin-left: 0.45rem;
      margin-top: 0.1rem;
      padding: 0.12rem 0.6rem;
      border-radius: 999px;
      border: 1px solid var(--color-teal);
      background: #E6FBFA;
      font-size: 0.75rem;
      color: #111827;
      white-space: nowrap;
    }
    .citation-pill a {
      color: inherit;
      text-decoration: none;
    }
    .citation-pill a:hover {
      text-decoration: underline;
    }
    .reformulated-box {
      padding: 0.5rem 0.75rem;
      margin-bottom: 0.5rem;
      border-radius: 10px;
      background: #F9FAFB;
      border: 1px solid rgba(148,163,184,0.8);
      font-size: 0.85rem;
      color: #111827;
    }

    /* Summary line layout for expand feature */
    .summary-line-main {
      display: block;
    }
    .summary-line-controls {
      margin-top: 0.35rem;
      display: flex;
      gap: 0.4rem;
      flex-wrap: wrap;
    }
    .expand-slot {
      margin-top: 0.4rem;
    }
    .expanded-block {
      padding: 0.5rem 0.6rem;
      border-radius: 10px;
      background: #F9FAFB;
      border: 1px solid rgba(148,163,184,0.6);
      font-size: 0.85rem;
      color: #111827;
    }
    .expanded-block ul {
      margin: 0.25rem 0 0.25rem 1.0rem;
      padding: 0;
    }

    /* Small expand buttons inside bullets */
    .expand-btn {
      border-radius: 999px;
      border: 1px solid var(--color-teal);
      background: #ffffff;
      color: var(--color-text-dark);
      padding: 0.15rem 0.7rem;
      font-size: 0.75rem;
      cursor: pointer;
      box-shadow: none;
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
    }
    .expand-btn::after {
      content: "⤵";
      font-size: 0.8rem;
      opacity: 0.8;
    }
    .expand-btn:hover:not(:disabled) {
      background: #E6FBFA;
    }
    .expand-btn:disabled {
      opacity: 0.6;
      cursor: default;
    }

    /* Grey "expand" pill for sections */
    .section-toggle {
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.6);
      background: #E5E7EB;
      color: var(--color-muted);
      padding: 0.15rem 0.7rem;
      font-size: 0.7rem;
      cursor: pointer;
      white-space: nowrap;
    }
    .section-toggle:hover {
      background: #D1D5DB;
    }

    .collapsible-body {
      margin-top: 0.4rem;
      display: none;
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <h1>
        CrAIg
        <span class="logo-pill">RETAIL MOSAIC Copilot</span>
      </h1>
      <div class="subtitle">
        Ask a question! CrAIg finds the right reports, summarises the key points and links you straight to the evidence.
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
        <!-- SUMMARY (always visible) -->
        <div class="results-section">
        <div class="results-header">
          <div class="results-header-main">
            <span>Summary</span>
            <span class="pill" id="summary-pill">Waiting for query…</span>
          </div>
          <!-- New small toggle button in the top-right of the bubble -->
          <button type="button" class="section-toggle" id="reformulate-toggle">
            Reformulation: On
          </button>
        </div>
        <div id="summary-content" class="summary-content">
          <em>No results yet.</em>
        </div>
      </div>



        <!-- SOURCES (collapsible) -->
        <div class="results-section">
          <div class="results-header">
            <div class="results-header-main">
              <span>Sources</span>
              <span class="pill" id="sources-count">0 docs</span>
            </div>
            <button type="button" class="section-toggle" id="sources-toggle">Show details ▾</button>
          </div>
          <div id="sources-body" class="collapsible-body">
            <div id="sources-list">
              <em>No sources yet.</em>
            </div>
          </div>
        </div>

        <!-- INLINE CITATIONS (collapsible) -->
        <div class="results-section">
          <div class="results-header">
            <div class="results-header-main">
              <span>Inline citations</span>
              <span class="pill" id="citations-count">0 refs</span>
            </div>
            <button type="button" class="section-toggle" id="citations-toggle">Show details ▾</button>
          </div>
          <div id="citations-body" class="collapsible-body">
            <div id="citations-list">
              <em>No inline citations yet.</em>
            </div>
          </div>
        </div>

        <!-- RAW JSON (collapsible) -->
        <div class="results-section">
          <div class="results-header">
            <div class="results-header-main">
              <span>Raw JSON</span>
            </div>
            <button type="button" class="section-toggle" id="raw-toggle">Show payload ▾</button>
          </div>
          <div id="raw-body" class="collapsible-body">
            <pre id="raw-json" class="raw-json">{}</pre>
          </div>
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

    const sourcesToggleEl = document.getElementById("sources-toggle");
    const citationsToggleEl = document.getElementById("citations-toggle");
    const rawToggleEl = document.getElementById("raw-toggle");
    const sourcesBodyEl = document.getElementById("sources-body");
    const citationsBodyEl = document.getElementById("citations-body");
    const rawBodyEl = document.getElementById("raw-body");

    const reformulateToggleEl = document.getElementById("reformulate-toggle");
    let reformulateEnabled = true;  // default behaviour = reformulate


    function escapeHtml(str) {
      return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    function setupSectionToggle(toggleEl, bodyEl, showLabel, hideLabel) {
      if (!toggleEl || !bodyEl) return;
      toggleEl.addEventListener("click", () => {
        const isHidden = bodyEl.style.display === "none" || bodyEl.style.display === "";
        if (isHidden) {
          bodyEl.style.display = "block";
          toggleEl.textContent = hideLabel;
        } else {
          bodyEl.style.display = "none";
          toggleEl.textContent = showLabel;
        }
      });
      // Start collapsed
      bodyEl.style.display = "none";
      toggleEl.textContent = showLabel;
    }

    function updateReformulateToggleLabel() {
      if (!reformulateToggleEl) return;
      reformulateToggleEl.textContent = reformulateEnabled
        ? "Reformulation: On"
        : "Reformulation: Off";
    }

    if (reformulateToggleEl) {
      reformulateToggleEl.addEventListener("click", () => {
        reformulateEnabled = !reformulateEnabled;
        updateReformulateToggleLabel();
      });
      // initialise label on load
      updateReformulateToggleLabel();
    }


    async function pollExpandJob(jobId, slotEl, btnEl) {
      if (btnEl) btnEl.disabled = true;

      try {
        while (true) {
          const resp = await fetch("/api/job/" + jobId);
          if (!resp.ok) {
            const text = await resp.text();
            slotEl.innerHTML = '<em style="color:#b91c1c;">Error: ' + escapeHtml(text) + '</em>';
            break;
          }
          const job = await resp.json();
          if (job.status === "done") {
            const result = job.result || {};
            const expSummary = result.summary || "";
            let innerHtml = "";

            if (expSummary) {
              // Format expansion like main bullets, but NO source markers or pills.
              const lines = expSummary
                .split("\\n")
                .map(l => l.trim())
                .filter(l => l.length > 0);

              const liHtml = lines.map(line => {
                let text = line.replace(/^\\s*-\\s*/, "");
                text = text.replace(/\\[S\\d+\\s+p\\d+[^\\]]*\\]/g, "");
                text = text.trim();
                return "<li>" + escapeHtml(text) + "</li>";
              }).join("");

              innerHtml = "<ul>" + liHtml + "</ul>";
            } else if (result.summary_html) {
              innerHtml = result.summary_html;
            } else {
              innerHtml = "<em>No expansion returned.</em>";
            }

            slotEl.innerHTML =
              '<div class="expanded-block">' +
                innerHtml +
              '</div>';
            break;
          } else if (job.status === "pending" || job.status === "processing") {
            await new Promise(r => setTimeout(r, 1200));
          } else {
            slotEl.innerHTML = '<em style="color:#b91c1c;">Error: unexpected status ' + escapeHtml(job.status) + '</em>';
            break;
          }
        }
      } catch (err) {
        console.error(err);
        slotEl.innerHTML = '<em style="color:#b91c1c;">Error: ' + escapeHtml(err && err.message ? err.message : String(err)) + '</em>';
      } finally {
        if (btnEl) btnEl.disabled = false;
      }
    }

    function wireExpandButtons() {
      document.querySelectorAll(".expand-btn").forEach(btn => {
        btn.addEventListener("click", async () => {
          const li = btn.closest("li");
          if (!li) return;
          const slot = li.querySelector(".expand-slot");
          const mainLine = li.querySelector(".summary-line-main");
          if (!slot || !mainLine) return;

          const docId = btn.getAttribute("data-doc-id");
          const bulletText = mainLine.innerText.trim();

          slot.innerHTML = "<em>Expanding this point…</em>";

          try {
            const resp = await fetch("/api/expand", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                doc_id: docId,
                bullet: bulletText
              })
            });

            if (!resp.ok) {
              const text = await resp.text();
              slot.innerHTML = '<em style="color:#b91c1c;">Error: ' + escapeHtml(text) + '</em>';
              return;
            }

            const data = await resp.json();
            const jobId = data.job_id;
            if (!jobId) {
              slot.innerHTML = '<em style="color:#b91c1c;">No job_id returned.</em>';
              return;
            }

            await pollExpandJob(jobId, slot, btn);
          } catch (err) {
            console.error(err);
            slot.innerHTML = '<em style="color:#b91c1c;">Error: ' + escapeHtml(err && err.message ? err.message : String(err)) + '</em>';
          }
        });
      });
    }

    function renderResult(data) {
      const sources = Array.isArray(data.sources) ? data.sources : [];
      const citations = Array.isArray(data.inline_citations) ? data.inline_citations : [];
      const summary = data.summary || "";

      const reformulated = !!data.reformulated;
      const usedQuery = data.used_query || "";


      // ===== 1) SUMMARY (bullets + citation pills + expand buttons) =====
      const summaryHtmlPieces = [];

      // If backend says the query was reformulated, show the sub-box at the top.
      if (reformulated && usedQuery) {
        summaryHtmlPieces.push(
          '<div class="reformulated-box">' +
            '<strong>Reformulated query:</strong> ' +
            escapeHtml(usedQuery) +
          '</div>'
        );
      }

      if (summary) {
        const lines = summary.split("\\n").filter(line => line.trim().length > 0);
        const itemsHtml = lines.map((line, idx) => {
          const bulletNum = idx + 1;

          // Clean text: strip "- " and raw [S# p# "..."] markers
          let text = line.replace(/^\\s*-\\s*/, "");
          text = text.replace(/\\[S\\d+\\s+p\\d+[^\\]]*\\]/g, "");
          text = text.trim();

          // Match citations for this bullet (handle numeric or string bullet field)
          const citsForBullet = citations.filter(c => {
            const bRaw = c.bullet;
            const b = bRaw != null ? parseInt(bRaw, 10) : NaN;
            return !Number.isNaN(b) && b === bulletNum;
          });

          // Decide primary doc_id for expansion from first citation
          let primaryDocId = null;
          if (citsForBullet.length > 0) {
            const first = citsForBullet[0];
            const sRaw = first.S;
            const sVal = sRaw != null ? parseInt(sRaw, 10) : NaN;
            if (!Number.isNaN(sVal) && sVal >= 1 && sVal <= sources.length) {
              const src = sources[sVal - 1];
              if (src && src.document_id != null) {
                primaryDocId = src.document_id;
              }
            }
          }

          // Citation pills (click to open PDF)
          const pillsHtml = citsForBullet.map(c => {
            const sRaw = c.S;
            const pRaw = c.page;

            const sVal = sRaw != null ? parseInt(sRaw, 10) : NaN;
            const pageNum = pRaw != null ? parseInt(pRaw, 10) : null;
            const quoteText = c.quote || "";

            let href = null;
            if (!Number.isNaN(sVal) && sVal >= 1 && sVal <= sources.length) {
              const src = sources[sVal - 1];
              if (src && src.url) {
                href = src.url;
                const params = [];
                if (pageNum != null && !Number.isNaN(pageNum)) {
                  params.push("page=" + encodeURIComponent(pageNum));
                }
                if (quoteText) {
                  params.push("quote=" + encodeURIComponent(quoteText));
                }
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
            if (!Number.isNaN(sVal)) {
              let refLabel = "S" + sVal;
              if (pageNum != null && !Number.isNaN(pageNum)) {
                refLabel += ", p" + pageNum;
              }
              labelParts.push(refLabel);
            }
            const label = labelParts.join(" — ");

            if (href) {
              return ' <span class="citation-pill"><a href="' + href +
                     '" target="_blank" rel="noopener noreferrer">' +
                     label + '</a></span>';
            } else {
              return ' <span class="citation-pill">' + label + '</span>';
            }
          }).join("");

          const expandBtnHtml = primaryDocId !== null
            ? '<button type="button" class="expand-btn" data-doc-id="' +
              primaryDocId + '" data-bullet-index="' + bulletNum + '">Expand</button>'
            : "";

          return (
            '<li data-bullet-index="' + bulletNum + '">' +
              '<div class="summary-line-main">' +
                escapeHtml(text) + pillsHtml +
              '</div>' +
              (expandBtnHtml
                ? '<div class="summary-line-controls">' + expandBtnHtml + '</div>'
                : '') +
              '<div class="expand-slot"></div>' +
            '</li>'
          );
        }).join("");

        summaryHtmlPieces.push("<ul>" + itemsHtml + "</ul>");
      } else {
        summaryHtmlPieces.push("<em>No summary returned.</em>");
      }

      summaryContentEl.innerHTML = summaryHtmlPieces.join("");
      // Wire expand buttons after rendering summary
      wireExpandButtons();


      // ===== 2) SOURCES =====
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

      // ===== 3) INLINE CITATIONS SIDEBAR =====
      citationsCountEl.textContent = citations.length + (citations.length === 1 ? " ref" : " refs");

      if (citations.length === 0) {
        citationsListEl.innerHTML = "<em>No inline citations yet.</em>";
      } else {
        citationsListEl.innerHTML = citations.map(c => {
          const bullet = c.bullet ?? "?";

          const sRaw = c.S;
          const pRaw = c.page;

          const sVal = sRaw != null ? parseInt(sRaw, 10) : NaN;
          const pageNum = pRaw != null ? parseInt(pRaw, 10) : null;
          const sIndex = !Number.isNaN(sVal) ? sVal - 1 : null;
          const quoteText = c.quote || "";

          const sLabel = !Number.isNaN(sVal) ? "S" + sVal : "?";
          const pageLabel = pageNum != null && !Number.isNaN(pageNum) ? "p" + pageNum : "?";
          const text =
            "• Bullet " + bullet + " → " + sLabel + " " + pageLabel +
            (quoteText ? ' – “' + escapeHtml(quoteText) + '”' : "");

          let href = null;
          if (
            sIndex != null &&
            sIndex >= 0 &&
            sIndex < sources.length &&
            sources[sIndex].url
          ) {
            href = sources[sIndex].url;
            const params = [];
            if (pageNum != null && !Number.isNaN(pageNum)) {
              params.push("page=" + encodeURIComponent(pageNum));
            }
            if (quoteText) {
              params.push("quote=" + encodeURIComponent(quoteText));
            }
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

      // ===== 4) RAW JSON DEBUG =====
      rawJsonEl.textContent = JSON.stringify(data, null, 2);

      // Wire expand buttons after rendering summary
      wireExpandButtons();
    }

    async function pollJob(jobId) {
      summaryPillEl.textContent = "Processing . . . ";
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
          statusEl.textContent = "Processing . . . ";
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
          body: JSON.stringify({
            q: q,
            top_k: 5,
            reformulate: reformulateEnabled
          })
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

    // Collapsible toggles for sections
    setupSectionToggle(
      sourcesToggleEl,
      sourcesBodyEl,
      "Show details ▾",
      "Hide details ▴"
    );
    setupSectionToggle(
      citationsToggleEl,
      citationsBodyEl,
      "Show details ▾",
      "Hide details ▴"
    );
    setupSectionToggle(
      rawToggleEl,
      rawBodyEl,
      "Show payload ▾",
      "Hide payload ▴"
    );

    // Let Enter in the question box trigger the search
    queryInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (typeof form.requestSubmit === "function") {
          form.requestSubmit();
        } else {
          form.dispatchEvent(new Event("submit", { cancelable: true, bubbles: true }));
        }
      }
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

    # new: optional reformulation flag from frontend
    reformulate = data.get("reformulate")
    if reformulate is None:
        reformulate_flag = True  # default behaviour
    else:
        reformulate_flag = bool(reformulate)

    if not q:
        return jsonify({"error": "Query cannot be empty"}), 400

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "query": q,
        "top_k": int(top_k),
        "status": "pending",
        "result": None,
        "reformulate": reformulate_flag,  # <-- stored on job
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


@app.route("/api/admin/job/<job_id>/upload_pdf", methods=["POST"])
def admin_upload_pdf(job_id: str):
    api_key = request.args.get("api_key")
    if api_key != ADMIN_API_KEY:
        return jsonify({"detail": "Invalid API key"}), 401

    doc_id = request.args.get("doc_id")
    if not doc_id:
        return jsonify({"detail": "Missing doc_id"}), 400

    if "file" not in request.files:
        return jsonify({"detail": "Missing file"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"detail": "Empty filename"}), 400

    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"detail": "Job not found"}), 404

        safe_name = secure_filename(f"{job_id}_{doc_id}.pdf")
        save_path = os.path.join(PDF_TMP_DIR, safe_name)
        file.save(save_path)

        pdf_paths = job.setdefault("pdf_paths", {})
        pdf_paths[str(doc_id)] = save_path

    return jsonify({"ok": True})

@app.route("/pdf/<job_id>/<doc_id>", methods=["GET"])
def serve_pdf(job_id: str, doc_id: str):
    # If a page query param is present, redirect to the same URL
    # but with a #page=... fragment so the browser PDF viewer jumps there.
    page = request.args.get("page")
    if page:
        # Build the same path without query and add the hash.
        # request.path is "/pdf/<job_id>/<doc_id>"
        target = f"{request.path}#page={page}"
        return redirect(target, code=302)

    # Normal behaviour: stream the PDF binary
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"detail": "Job not found"}), 404

        pdf_paths = job.get("pdf_paths") or {}
        path = pdf_paths.get(str(doc_id))

    if not path or not os.path.exists(path):
        return jsonify({"detail": "PDF not found"}), 404

    return send_file(path, mimetype="application/pdf")


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


@app.route("/api/expand", methods=["POST"])
def submit_expand():
    data = request.get_json(silent=True) or {}
    bullet = (data.get("bullet") or "").strip()
    doc_id = data.get("doc_id")

    if not bullet:
        return jsonify({"error": "Bullet text cannot be empty"}), 400
    if doc_id is None:
        return jsonify({"error": "Missing doc_id"}), 400

    try:
        doc_id_int = int(doc_id)
    except ValueError:
        return jsonify({"error": "doc_id must be an integer"}), 400

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "job_type": "expand_bullet",  # distinct from the normal search
        "query": bullet,              # store the bullet text here
        "doc_id": doc_id_int,
        "status": "pending",
        "result": None,
    }

    with jobs_lock:
        jobs[job_id] = job

    return jsonify({"job_id": job_id})



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
                      "job_type": job.get("job_type", "search"),
                      "query": job.get("query"),
                      "doc_id": job.get("doc_id"),
                      "top_k": job.get("top_k", 5),
                      "reformulate": job.get("reformulate", True),  # <-- new
                      "status": job["status"],
                  }
              )

    return jsonify({"id": None, "status": "idle"})