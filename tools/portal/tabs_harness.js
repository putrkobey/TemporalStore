/* Run a portal page's tabs the way someone using them would, with a mouse and with a keyboard.
 *
 * The markup cannot answer the questions that matter. Whether a tablist responds to an arrow key,
 * whether exactly one pane is ever visible, whether the tab a screen reader reports as selected is
 * the pane actually on screen -- all of that is behaviour, and a page whose tabs are wired to
 * nothing looks in source exactly like a page whose tabs work. The panel on the key portal shipped
 * broken three times for precisely that reason.
 *
 * So the tab buttons and panes are read out of the real page, the real helper is executed against
 * them, and events are dispatched through the listeners it registered.
 *
 * Usage: node tabs_harness.js <page.html>
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

/* ---------- the page's own tabs and panes ---------- */
function attr(tag, name) {
  const m = tag.match(new RegExp(name + '="([^"]*)"'));
  return m ? m[1] : null;
}

const tabTags = [...page.matchAll(/<button[^>]*role="tab"[\s\S]*?>/g)].map((m) => m[0]);
const paneTags = [...page.matchAll(/<section[^>]*class="pane"[\s\S]*?>/g)].map((m) => m[0]);

if (!tabTags.length) { console.log("FAIL the page declares no tabs"); process.exit(1); }

/* ---------- a DOM just wide enough ---------- */
function makeEl(spec) {
  const listeners = {};
  const attrs = Object.assign({}, spec.attrs);
  return {
    id: spec.id,
    hidden: !!spec.hidden,
    tabIndex: 0,
    focused: false,
    dataset: Object.assign({}, spec.dataset),
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
