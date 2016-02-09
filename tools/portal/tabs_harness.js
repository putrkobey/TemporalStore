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
    dispatch(type, event) { (listeners[type] || []).forEach((fn) => fn(event || {})); },
    listens(type) { return (listeners[type] || []).length > 0; },
    setAttribute(k, v) { attrs[k] = String(v); },
    getAttribute(k) { return k in attrs ? attrs[k] : null; },
    focus() { this.focused = true; },
    /* Real buttons have this, and showTab uses it to reach the same path a person's click takes. */
    click() { this.dispatch("click"); },
  };
}

const tabs = tabTags.map((tag) => makeEl({
  id: attr(tag, "id"),
  dataset: { pane: attr(tag, "data-pane") },
  attrs: { "aria-selected": attr(tag, "aria-selected") || "false" },
}));
const panes = paneTags.map((tag) => makeEl({ id: attr(tag, "id"), hidden: /\shidden[\s>]/.test(tag) }));

const byId = new Map();
tabs.concat(panes).forEach((el) => byId.set(el.id, el));

const doc = {
  querySelectorAll(selector) {
    if (selector === ".tabs button") { return tabs; }
    if (selector === ".pane") { return panes; }
    return [];
  },
  getElementById(id) { return byId.get(id) || null; },
};

/* ---------- the page's own helper ---------- */
/* The DEFINITION, not a call. "window.wireTabs" also matches the page calling it, and which
   block comes first differs between generated pages (helper injected before the page script)
   and hand-maintained ones (appended before </body>), so matching loosely picks a different
   block per page and runs the page instead of the helper. */
const src = scripts.find((s) => s.includes("window.wireTabs = function"));
if (!src) { console.log("FAIL the page carries no tab helper"); process.exit(1); }
const win = {};
new Function("window", "document", src)(win, doc);
ok("the page defines wireTabs", typeof win.wireTabs === "function");
ok("the page defines markTab", typeof win.markTab === "function");

/* ---------- structure: every tab has its pane, and the reverse ---------- */
const paneIds = new Set(panes.map((p) => p.id));
const missing = tabs.filter((t) => !paneIds.has("pane-" + t.dataset.pane));
ok("every tab points at a pane that exists", missing.length === 0,
   missing.map((t) => t.id + " -> pane-" + t.dataset.pane).join(", "));
const tabTargets = new Set(tabs.map((t) => "pane-" + t.dataset.pane));
