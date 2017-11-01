/* Run the "awaiting restart" panel: what it names, and where clicking a name goes.
 *
 * The strip counts the settings waiting for a restart from every page. Naming them is this panel's
 * whole job, and taking somebody to one is harder than scrolling: four of the nine groups render
 * shut, and 26 of the 47 restart-scoped settings live in one of them. A name that scrolls to a
 * folded row lands on a closed triangle, which looks exactly like a working link.
 *
 * Extracted and run rather than read, in the manner of the byte-hint harness beside this one: a
 * renderer that never opens the group reads the same in a diff as one that does.
 *
 * Usage: node awaiting_restart_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function extract(marker) {
  const start = page.indexOf(marker);
  if (start < 0) { throw new Error("not on this page: " + marker); }
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  throw new Error("unbalanced: " + marker);
}

/* ---------- a DOM with real containment ---------- */
/* Flat stubs agree with whatever they are told, and everything here is a question about what is
   inside what, so the nodes below carry a parent and answer `closest` by walking it. */
function node(tag, attrs) {
  const el = {
    tagName: tag.toUpperCase(),
    parent: null,
    children: [],
    attrs: attrs || {},
    hidden: false,
    open: false,
    innerHTML: "",
    scrolled: false,
    focused: false,
    classes: new Set(),
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name)
      ? this.attrs[name] : null; },
    scrollIntoView() { this.scrolled = true; },
    focus() { this.focused = true; },
    append(child) { child.parent = this; this.children.push(child); return child; },
    closest(selector) {
      let cursor = this;
