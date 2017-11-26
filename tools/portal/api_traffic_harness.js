/* Run the API page and check each route row reports what that route has served.
 *
 * The page builds its rows as an HTML string, so a stub whose querySelectorAll returns nothing
 * would let every assertion pass without a single cell being painted. The routes container parses
 * what the page assigns to it and hands back real cells, which is the only way the painting is
 * actually exercised.
 *
 * Usage: node api_traffic_harness.js <api_portal.html>
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

/* Two documented routes sharing one counter, and one with a counter of its own. */
const ROUTES = [
  { group: "Memory", method: "GET", path: "/v1/memories", summary: "List memories.",
    scope: "read", metric: "/v1/memories" },
  { group: "Memory", method: "GET", path: "/v1/memory/{id}", summary: "One memory.",
    scope: "read", metric: "/v1/memory/{id}",
    metric_shared_with: ["/v1/memory/{id}/history"] },
  { group: "Memory", method: "GET", path: "/v1/memory/{id}/history", summary: "Its history.",
    scope: "read", metric: "/v1/memory/{id}",
    metric_shared_with: ["/v1/memory/{id}"] },
  { group: "Admin", method: "GET", path: "/v1/admin/scopes", summary: "Scopes.",
    scope: "admin", metric: "/v1/admin/scopes" },
];

const FRAME = {
  ts: 1, imports: {}, warnings: 0, embedding: { total: 1 },
  traffic: {
    total_requests: 9, total_errors: 2, in_flight: 0,
    routes: {
      "/v1/memories": { requests: 7, errors: 0, avg_ms: 4.5 },
      "/v1/memory/{id}": { requests: 2, errors: 2, avg_ms: 11 }
    }
  }
};

let cells = [];
function parseCells(html) {
  cells = [...String(html || "").matchAll(/<span class="route-live" data-metric="([^"]*)"([^>]*)>/g)]
    .map((m) => ({
      metric: m[1], attrs: m[2], textContent: "", className: "route-live",
      getAttribute(k) { return k === "data-metric" ? this.metric : null; }
    }));
}

function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = v; if (this.id === "routes") { parseCells(v); } },
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}

const els = new Map();
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

const win = {
  document: {
    getElementById: (id) => el(id),
    querySelector: () => makeEl(),
    querySelectorAll: (sel) => (sel === ".route-live" ? cells : []),
    createElement: () => makeEl(), addEventListener() {}, body: makeEl(), hidden: false
  },
  addEventListener() {},
  location: { origin: "http://localhost", href: "http://localhost", reload() {} },
  sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  navigator: { clipboard: { writeText() {} } },
  setTimeout: () => 0, setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    if (String(url).indexOf("/v1/admin/routes") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               json: () => Promise.resolve({ status: "ok", routes: ROUTES }) });
    }
    return new Promise(() => {});
  }
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw: " + e.message); failures += 1; }
});

setTimeout(function () {
