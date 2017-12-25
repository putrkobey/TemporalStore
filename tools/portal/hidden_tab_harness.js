/* Does a portal page in a background tab actually let go of its live connection?
 *
 * Every page says it does. The status strip carries the rule in as many words -- "A hidden tab
 * holds a connection open for nobody. Drop it, reconnect on return." -- and aborts the request
 * when the tab is hidden. What it does not do is stop the reconnect: aborting rejects the fetch,
 * the catch schedules setTimeout(open, backoff), and about a second later the page opens the
 * stream again while the tab is still hidden.
 *
 * That is not visible in the source. The abort is there, the comment is there, and the line that
 * undoes them is thirty lines away in a different function. So the page's own code is run here
 * against a fake clock: hide the tab, fire every timer that comes due, and count the connections
 * opened after the abort.
 *
 * Usage: node hidden_tab_harness.js <page.html>
 */
"use strict";
const fs = require("fs");
/* Captured before anything in this file shadows it: the page under test is handed a fake
   setTimeout, and the flush below must not go through it. */
const realSetImmediate = setImmediate;

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---------- a fake clock, so a backoff does not cost a real second ---------- */
function makeWorld() {
  const timers = [];
  let now = 0;
  const opened = [];
  const aborted = [];

  const listeners = { document: {}, window: {} };
  /* Enough of an element for the strip to read a key out of and to write a status into. It reads
     `key()` before connecting and gives up quietly when there is none, so a stub returning null
     produces a page that simply never opens a connection -- which would make every check below
     pass for the wrong reason. */
  function element(id) {
    return {
      id,
      value: "k-test",
      textContent: "",
      className: "",
      hidden: false,
      style: {},
      dataset: {},
      classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
      addEventListener() {},
      setAttribute() {},
      getAttribute: () => null,
      appendChild() {},
      querySelector: () => null,
      querySelectorAll: () => [],
    };
  }
  const nodes = {};
  const doc = {
    hidden: false,
    addEventListener(type, fn) { (listeners.document[type] = listeners.document[type] || []).push(fn); },
    getElementById(id) { return (nodes[id] = nodes[id] || element(id)); },
    createElement(tag) { return element(tag); },
    querySelectorAll() { return []; },
    querySelector() { return null; },
    body: element("body"),
  };
  const win = {
    addEventListener(type, fn) { (listeners.window[type] = listeners.window[type] || []).push(fn); },
    location: { hash: "" },
    setTimeout(fn, ms) { timers.push({ at: now + (ms || 0), fn }); return timers.length; },
    clearTimeout() {},
    setInterval() { return 0; },
    clearInterval() {},
  };

  class FakeController {
    constructor() {
      this.signal = { aborted: false };
      this.index = opened.length;
    }
    abort() {
      this.signal.aborted = true;
      aborted.push(this.index);
      const pending = opened[this.index];
      if (pending && pending.reject) { pending.reject(new Error("aborted")); }
    }
  }

  function fetchStub(url) {
    const record = { url, reject: null };
    opened.push(record);
    /* A connection that never resolves: a live stream does not settle until it breaks, and that
       is exactly the state we want the page to be holding when the tab is hidden. */
    return new Promise((_resolve, reject) => { record.reject = reject; });
  }

  return {
    doc, win, opened, aborted, listeners,
    fetch: fetchStub,
    AbortController: FakeController,
    fire(type, where) {
      (listeners[where][type] || []).forEach((fn) => fn({}));
    },
    /* Run every timer due within `ms`, including ones those timers schedule.
     *
     * The flush matters more than the clock. Aborting a fetch rejects it, and the reconnect is
     * registered from the .catch at the end of that rejection's microtask chain -- so a scan for
     * due timers that runs first finds none and reports a page that let go of its connection.
     * That is a false pass on exactly the behaviour this file exists to check, and it is what a
     * single `await Promise.resolve()` produced. setImmediate yields past the whole microtask
