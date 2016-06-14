/* Feed the shared strip a frame and read what the "awaiting restart" segment did.
 *
 * The strip's render() is wrapped by its caller in a try/catch that ignores errors, so a segment
 * that throws is a segment that silently does nothing -- and that block defines only n, show and
 * plural, so reaching for any other helper fails exactly that way and looks identical to a healthy
 * deployment with nothing waiting. Reading the source cannot tell those apart.
 *
 * Usage: node waiting_segment_harness.js <page.html>
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

const els = new Map();
function makeEl(id) {
  return {
    /* The same shape the datanode segment's harness uses: the pages read an admin key out of a
       field and trim it, so a stub without `value` throws at load and every check below then
       reports on a strip that never ran. */
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false,
    style: {}, dataset: {}, href: "", files: [], options: [], children: [],
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
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {}, Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: () => new Promise(() => {})
};
win.window = win;

const names = Object.keys(win);
const values = names.map((k) => win[k]);
scripts.forEach((body, index) => {
  try { new Function(...names, body)(...values); }
  catch (e) { console.log("FAIL script " + index + " threw at load: " + e.message); failures += 1; }
});

ok("the strip exposes a way to feed it a frame",
   typeof win.__matrixarkLiveFrame === "function");

function feed(waiting) {
  const seg = el("liveWaiting");
  seg.hidden = true;
  seg.innerHTML = "";
  const frame = { ts: 1, traffic: { total_requests: 1 }, imports: {}, warnings: 0,
                  embedding: { total: 1 } };
  if (waiting !== undefined) { frame.settings_waiting = waiting; }
  win.__matrixarkLiveFrame(frame);
  return seg;
}

let seg = feed(2);
ok("settings waiting are shown", !seg.hidden && /awaiting restart/.test(seg.innerHTML),
