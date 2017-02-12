/* Does an open overview page re-read when the configuration changes, and only then?
 *
 * Re-reading costs three listings on the backend, so "reacts to a change" and "reacts to every
 * frame" are very different behaviours that look nearly identical in source. The page's own stream
 * delivers a sequence of frames here and the harness counts what it fetched.
 *
 * Usage: node config_change_harness.js <overview_portal.html>
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

function frame(changedAt) {
  const f = { ts: Date.now() / 1000, traffic: { total_requests: 1 }, imports: {}, warnings: 0,
              embedding: { total: 1 } };
  if (changedAt !== undefined) { f.config_changed_at = changedAt; }
  return "event: status\ndata: " + JSON.stringify(f) + "\n\n";
}


const overviewReads = [];
const seenFetches = [];
const els = new Map();
const hidden = { value: false };

function makeEl(id) {
  return {
    id: id || "", hidden: false, innerHTML: "", textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
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

function json(body) {
  return Promise.resolve({ ok: true, status: 200,
                           json: () => Promise.resolve(body),
                           text: () => Promise.resolve(JSON.stringify(body)) });
}

/* Frames are released one at a time by the harness, not on a timer. Two attempts at timing this
   produced two different wrong answers -- first "0 re-reads for one change" about a page that had
   re-read correctly, then the re-read attributed to the wrong phase. Whether a frame has been
   PROCESSED is the thing being waited on, and a clock only approximates it. */
let release = null;
function streaming() {
  return Promise.resolve({
    ok: true, status: 200,
    body: { getReader: () => ({ read: () => new Promise((r) => { release = r; }) }) },
    json: () => Promise.resolve({}), text: () => Promise.resolve("")
  });
}

function settle() {
  return new Promise((r) => setImmediate(() => setImmediate(r)));
}

async function deliver(text) {
  while (!release) { await settle(); }
  const r = release;
  release = null;
  r({ done: false, value: Buffer.from(text, "utf8") });
