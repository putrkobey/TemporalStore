/* Do the pages that ask for live frames actually get them?
 *
 * `window.__matrixarkOnFrame` is defined by the shared nav script, which `emit` places AFTER the
 * page's own script -- deliberately, because the nav block checks whether the page has already
 * claimed the stream, and running first would give some pages two.
 *
 * That ordering means a page registering at the top level of its own script calls a function that
 * does not exist yet. Guarded with `if (window.__matrixarkOnFrame)`, the guard is false and the
 * watcher is never registered: the feature is gone, in source, silently.
 *
 * page_watchers_harness cannot see this -- its stub defines __matrixarkOnFrame up front, so every
 * page looks like it registers. This runs the scripts in document order against a window that
 * starts without it, as a browser does, and asks of each script that tries to register: was the
 * function there when you called it?
 *
 * Usage: node frame_registration_harness.js <page.html> [...]
 */
"use strict";
const fs = require("fs");

function makeEl(id) {
  return {
    hidden: true, innerHTML: "", textContent: "",
    /* The nav stream is inert without a key, like every page. A harness that left this empty would
       see no frames and blame the wiring. */
    value: id === "key" ? "k-admin" : "",
    checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
}

const REGISTERS = /window\.__matrixarkOnFrame\s*\(/;
const QUEUES = /__matrixarkFrameQueue/;

function inspect(path) {
  const page = fs.readFileSync(path, "utf8");
  const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

  /* Deliberately absent. Whether the page finds it is the whole question. */
  const win = {
    document: {
      getElementById: (id) => makeEl(id), querySelector: () => makeEl(), querySelectorAll: () => [],
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

  /* A stream that answers, so the nav block reaches its watchers. Registering is not the same as
     receiving, and the difference is the whole point of the fix. */
  const FRAME = 'event: status' + String.fromCharCode(10) + 'data: {"ts":1,"traffic":{"routes":{}},' +
                '"imports":{},"warnings":0,"embedding":{"total":7}}' +
                String.fromCharCode(10) + String.fromCharCode(10);
  let sent = false;
  win.fetch = (url) => {
    const u = String(url);
    if (u.indexOf("/v1/admin/events") < 0) {
      /* Answer, rather than hang. Setup and Overview run their own stream and only start it after
         a config load succeeds -- against a fetch that never resolves they never open one, and the
         harness would report a wiring failure that belongs to the harness. */
      return Promise.resolve({ ok: true, status: 200,
                               json: () => Promise.resolve({ settings: {} }),
                               text: () => Promise.resolve("{}") });
    }
    return Promise.resolve({
      ok: true, status: 200,
      body: { getReader: () => ({ read: () => {
        if (sent) { return new Promise(() => {}); }
        sent = true;
        /* Delivered on a real timer, not immediately. Resolved promises run on the microtask queue
           and would deliver the only frame before the probe watcher below has registered -- the
           harness would then report a wiring failure that is its own. */
        return new Promise((res) => setTimeout(
          () => res({ done: false, value: Buffer.from(FRAME, "utf8") }), 5));
      } }) }
    });
  };
  win.TextDecoder = function () { this.decode = (v) => Buffer.from(v || []).toString("utf8"); };
