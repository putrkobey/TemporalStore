/* Run the catalogue page's Skills/Resources tabs.
 *
 * One filter covers both lists while only one is visible, so the interesting behaviour is what
 * happens when the query matches the tab you are NOT looking at. Reading the source cannot tell a
 * page that says so from one that shows an empty list.
 *
 * Usage: node catalog_tabs_harness.js <catalog_portal.html>
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

const SKILLS = { skills: [
  { name: "deploy-runbook", description: "How to deploy the airship", updated_at: 1 },
  { name: "oncall", description: "Who to wake", updated_at: 2 },
] };
const RESOURCES = { resources: [
  { name: "hull-spec.md", description: "Airship hull specification", updated_at: 3 },
] };

const listeners = {};
const els = new Map();

function attr(tag, name) {
  const m = tag.match(new RegExp(name + '="([^"]*)"'));
  return m ? m[1] : null;
}

/* Tabs and panes come from the STATIC markup, so they are parsed from the page once. */
const tabCells = [...page.matchAll(/<button[^>]*role="tab"[\s\S]*?<\/button>/g)].map((m) => {
  const tag = m[0];
  const el = {
    id: attr(tag, "id"), dataset: { pane: attr(tag, "data-pane") },
    tabIndex: 0, focused: false, _listeners: {},
    _attrs: { "aria-selected": attr(tag, "aria-selected") || "false" },
    addEventListener(t, fn) { (this._listeners[t] = this._listeners[t] || []).push(fn); },
    dispatch(t, ev) { (this._listeners[t] || []).forEach((fn) => fn(ev || {})); },
    setAttribute(k, v) { this._attrs[k] = String(v); },
    getAttribute(k) { return k in this._attrs ? this._attrs[k] : null; },
    focus() { this.focused = true; }, click() { this.dispatch("click"); }
  };
  return el;
});
const panes = [...page.matchAll(/<section[^>]*class="pane"[^>]*>/g)].map((m) => ({
  id: attr(m[0], "id"), hidden: /\shidden[\s>]/.test(m[0])
}));

function makeEl(id) {
  return {
    id: id || "", hidden: false, innerHTML: "", textContent: "", value: "", className: "",
    style: {}, dataset: {}, children: [],
    addEventListener(type, fn) { listeners[(id || "") + ":" + type] = fn; },
    removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
}
function el(id) {
  const fromMarkup = tabCells.filter((t) => t.id === id)[0];
  if (fromMarkup) { return fromMarkup; }
