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
      while (cursor) {
        if (selector === "details" && cursor.tagName === "DETAILS") { return cursor; }
        if (selector === ".field" && cursor.classes.has("field")) { return cursor; }
        cursor = cursor.parent;
      }
      return null;
    },
    querySelector(selector) {
      const key = /\[data-key="([^"]+)"\]/.exec(selector);
      const walk = (n) => {
        if (key && n.attrs["data-key"] === key[1]) { return n; }
        for (const child of n.children) {
          const found = walk(child);
          if (found) { return found; }
        }
        return null;
      };
      return walk(this);
    },
  };
  el.classList = {
    add: (c) => el.classes.add(c),
    remove: (c) => el.classes.delete(c),
    contains: (c) => el.classes.has(c),
  };
  return el;
}

/* The shape the settings pane really has: a shut advanced group, holding a field row, holding the
   control. */
const groups = node("div");
const group = groups.append(node("details"));
group.open = false;
const field = group.append(node("div"));
field.classes.add("field");
field.classes.add("hidden");
const control = field.append(node("select", { "data-key": "ingestion.bulk_ingest" }));

const box = node("div");
box.hidden = true;
const saveMsg = node("div");

const byId = { groups, awaitingRestart: box, saveMsg };
const shown = [];
const said = [];

const $ = (id) => byId[id] || null;
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const say = (el, text) => said.push(text);
const window_ = { showTab: (pane) => shown.push(pane) };

const build = new Function("$", "esc", "say", "window",
  extract("function showSetting(key)") + "\n" +
  extract("function renderAwaiting(keys)") + "\n" +
  "return { showSetting: showSetting, renderAwaiting: renderAwaiting };");
