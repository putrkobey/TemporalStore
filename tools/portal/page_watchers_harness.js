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
