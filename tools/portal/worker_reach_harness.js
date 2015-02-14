/* Save a setting and read how far the page says the write reached.
 *
 * A live setting is read from the environment per call, and a write touches the environment of the
 * one worker that served it. With several workers the others keep the old value until they
 * restart, so "Saved and live now" is true of a fraction of the traffic. Whether the page says so
 * depends on what comes back from the write, which is behaviour rather than text.
 *
 * Usage: node worker_reach_harness.js <setup_portal.html> [workers]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const WORKERS = process.argv[3] === undefined ? 4 : Number(process.argv[3]);
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const els = new Map();
const listeners = {};
function makeEl(id) {
  return {
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false, disabled: false,
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { listeners[(id || "") + ":" + type] = fn; },
    removeEventListener() {}, appendChild() {}, removeChild() {},
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

function snapshot() {
  return {
    status: "ok", warnings: [],
    extraction: { provider: "deterministic" }, embedding: { provider: "deterministic" },
    settings: {
      status: "ok",
      groups: { Extraction: [
        { key: "extraction.model", kind: "str", label: "Model", help: "", value: "before",
          env: "MATRIXARK_EXTRACTION_MODEL", applies: "live", source: "config", essential: false,
          default: "", overridable_by: [], boot_pinned: false, pending_restart: false }
      ] },
      group_meta: {}, presets: [], history: [], inventory: {}, essential_keys: [],
      config_file: "/tmp/runtime.json", pending_restart: [], unknown_stored: []
    }
  };
}

function reply(body) {
  return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body),
                           text: () => Promise.resolve(JSON.stringify(body)) });
}

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
  setTimeout: (fn) => { if (typeof fn === "function") { fn(); } return 0; },
  setInterval: () => 0, clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: (url, opts) => {
    const method = ((opts || {}).method || "GET").toUpperCase();
    if (String(url).indexOf("/v1/admin/config") >= 0 && method === "POST") {
      const body = { status: "ok", applied: [], restart_required: [] };
      if (WORKERS) { body.workers = WORKERS; }
      return reply(body);
    }
    if (String(url).indexOf("/v1/admin/config") >= 0) { return reply(snapshot()); }
    return new Promise(() => {});
  }
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw at load: " + e.message); failures += 1; }
});
