/* Run a portal page's scripts and report which of them register a live-frame watcher.
 *
 * The source-level check this replaces was satisfied by `if (false) { window.__matrixarkOnFrame(...) }`
 * -- the call is present and never happens. Running the script is the only way to tell those apart.
 *
 * The stub answers whatever a page asks of the DOM rather than modelling it: these scripts wire
 * handlers at load and do their work later, so nothing here needs to be faithful, only present.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

function makeEl() {
  const el = {
    hidden: true, innerHTML: "", textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {}, classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}

const registered = [];
/* Held by reference. The nav script drains this and then REPLACES window.__matrixarkFrameQueue
   with its own pusher, so reading that property afterwards finds the replacement and not what
   the page put in. The array itself still holds it. */
const queued = [];
const win = {
  document: {
    getElementById: () => makeEl(),
    querySelector: () => makeEl(),
    querySelectorAll: () => [],
    createElement: () => makeEl(),
    addEventListener() {},
    body: makeEl(),
    hidden: false
  },
  addEventListener() {},
  location: { origin: "http://localhost", href: "http://localhost", reload() {} },
  /* Every browser API a page actually reaches for is listed, rather than answered by a catch-all
     proxy: a proxy would also swallow a call into something that does not exist, which is the
     class of bug this harness is for. A gap here is worth one round trip to learn about. */
  sessionStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  localStorage: { getItem: () => "", setItem() {}, removeItem() {} },
  navigator: {},
  setTimeout: () => 0,
  setInterval: () => 0,
  clearInterval() {},
  Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
  encodeURIComponent, decodeURIComponent, parseInt, parseFloat,
  TextDecoder: function () { this.decode = () => ""; },
  AbortController: function () { this.abort = () => {}; this.signal = {}; },
  FileReader: function () {},
  Blob: function () {},
  URL: { createObjectURL: () => "", revokeObjectURL() {} },
  fetch: () => new Promise(() => {}),          /* never resolves: nothing here needs a response */
  __matrixarkOnFrame(callback) { registered.push(callback); },
  /* Pages register by pushing here, not by calling __matrixarkOnFrame directly: the nav script
     that defines that function is emitted after the page's own, so calling it at page-script time
     registered nothing at all. Counting only direct calls made this harness report a watcher for
