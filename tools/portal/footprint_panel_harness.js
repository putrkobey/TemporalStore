/* Run the Setup page, hand it a footprint, and read what it drew.
 *
 * Nothing here is provable from source. The renderer exists whether or not anything calls it, a
 * figure written into the wrong cell looks the same in a diff as one written into the right one,
 * and the case that matters most -- an engine that publishes no footprint -- differs from the
 * healthy case only in whether a dash or a zero reaches the page.
 *
 * Usage: node footprint_panel_harness.js <setup_portal.html> [published|absent|multiworker]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const mode = process.argv[3] || "published";
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const WORKER = {
  resident_bytes: 412 * 1048576,
  peak_bytes: 455 * 1048576,
  source: "/proc/self/status",
  workers: mode === "multiworker" ? 4 : 1
};
const ENGINE = mode === "absent" ? { available: false } : {
  available: true,
  cache_memory_bytes: 96 * 1048576,
  cache_disk_bytes: 12 * 1048576,
  cache_compression_saved_bytes: 3 * 1048576,
  store_logical_bytes: 6 * 1073741824,
  store_physical_bytes: 2 * 1073741824,
  store_compression_ratio: 3.0
};
const OVERVIEW = { status: "ok", footprint: { worker: WORKER, engine: ENGINE } };

const els = new Map();
function makeEl(id) {
  return {
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
}
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

let overviewCalls = 0;

const win = {
  document: {
    getElementById: (id) => el(id),
    querySelector: () => makeEl(), querySelectorAll: () => [],
    createElement: () => makeEl(), addEventListener() {}, body: makeEl(), hidden: false
  },
  addEventListener() {},
  location: { origin: "http://localhost", href: "http://localhost", reload() {} },
  sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  navigator: {},
  setTimeout: () => 0, setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url) => {
    const u = String(url);
    if (u.indexOf("/v1/admin/overview") >= 0) {
      overviewCalls += 1;
      return Promise.resolve({ ok: true, status: 200,
                               json: () => Promise.resolve(OVERVIEW),
                               text: () => Promise.resolve(JSON.stringify(OVERVIEW)) });
    }
    if (u.indexOf("/v1/admin/events") >= 0) {
      return Promise.resolve({ ok: true, status: 200,
                               body: { getReader: () => ({ read: () => new Promise(() => {}) }) },
                               json: () => Promise.resolve({}), text: () => Promise.resolve("") });
    }
    return Promise.resolve({ ok: true, status: 200,
                             json: () => Promise.resolve({ settings: {} }),
                             text: () => Promise.resolve("") });
  }
};
