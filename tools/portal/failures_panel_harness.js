/* Run the Setup page, hand it a frame carrying failures, and read what it drew.
 *
 * The panel is fed by the page's own stream, so nothing here is provable from source: the renderer
 * exists whether or not anything ever calls it, and a status breakdown rendered into the wrong cell
 * looks the same in a diff as one rendered into the right one.
 *
 * Usage: node failures_panel_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const NOW = Math.floor(Date.now() / 1000);
const FRAME = {
  ts: NOW, imports: {}, warnings: 0, embedding: { total: 4, encoded: 4, pending: 0, encoder: {} },
  traffic: {
    total_requests: 12, total_errors: 3, in_flight: 0,
    routes: {
      "/v1/memories": { requests: 9, errors: 1, avg_ms: 4.2, p95_ms: 10,
                        statuses: { "200": 8, "404": 1 } },
      "/v1/retrieve": { requests: 3, errors: 2, avg_ms: 30, p95_ms: null,
                        statuses: { "200": 1, "401": 1, "503": 1 } }
    },
    recent_failures: [
      /* A backend failure carries the token its exception was logged under; a refusal does not,
         because nothing went wrong inside to log. Both shapes are here on purpose. */
      { at: NOW - 5, route: "/v1/retrieve", method: "POST", status: 503,
        incident: "9f2c41ab77de" },
      { at: NOW - 400, route: "/v1/retrieve", method: "POST", status: 401 },
      { at: NOW - 7200, route: "/v1/memories", method: "GET", status: 404 }
    ]
  }
};

const els = new Map();
function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [], _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = String(v); },
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

const SSE = "event: status\ndata: " + JSON.stringify(FRAME) + "\n\n";

function streamingResponse() {
  let sent = false;
  return Promise.resolve({
    ok: true, status: 200,
    body: { getReader: () => ({ read: () => {
      if (sent) { return new Promise(() => {}); }
      sent = true;
      return new Promise((res) => setTimeout(
        () => res({ done: false, value: Buffer.from(SSE, "utf8") }), 5));
    } }) },
    json: () => Promise.resolve({}), text: () => Promise.resolve("")
  });
}

