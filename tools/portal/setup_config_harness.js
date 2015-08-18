/* When someone else changes the configuration, what does the Setup page do?
 *
 * Two answers, and picking the wrong one loses work. With nothing unsaved, taking their change is
 * right -- the form is showing stale values. With unsaved edits, reloading would discard what the
 * person here has typed, so it must say something instead and leave the edits alone.
 *
 * Both paths run through the same handler and differ only by a condition, which is exactly the
 * kind of thing that reads correct and behaves wrong. Frames are released on demand rather than on
 * a timer, because what is being waited on is a frame having been PROCESSED.
 *
 * Usage: node setup_config_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");
process.on("unhandledRejection", (e) => {
  console.log("  unhandled rejection: " + String(e && e.message ? e.message : e));
});

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const FIELD = { key: "extraction.provider", value: "deterministic", kind: "text",
                label: "Extraction provider",
                /* The page does `f.help.length` unguarded, so a field without help throws and the
                   error surfaces as "Could not reach the gateway". The server always sends it. */
                help: "Which provider extracts memories.", env: "MATRIXARK_EXTRACTION_PROVIDER",
                applies: "live", group: "models" };
const CONFIG = { settings: { groups: { models: [FIELD] },
                             group_meta: { models: { title: "Models", order: 1 } },
                             updated_at: 1000, config_file: "/tmp/runtime.json" } };

function frame(changedAt) {
  const f = { ts: Date.now() / 1000, traffic: { routes: {} }, imports: {}, warnings: 0,
              embedding: { total: 1, encoded: 1, pending: 0, encoder: {} } };
  if (changedAt !== undefined) { f.config_changed_at = changedAt; }
  return "event: status\ndata: " + JSON.stringify(f) + "\n\n";
}

const configReads = [];
const seenUrls = [];
const listeners = {};
const els = new Map();

function makeEl(id) {
  const el = {
    id: id || "", hidden: false, textContent: "", _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = String(v); },
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { (listeners[(id || "") + ":" + type] = fn); },
    removeEventListener() {}, appendChild() {}, removeChild() {},
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

let release = null;
