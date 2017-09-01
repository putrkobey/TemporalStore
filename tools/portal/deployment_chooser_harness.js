/* Run the Setup page and drive the deployment chooser the way a customer would.
 *
 * The thing worth proving here cannot be read off the source. The chooser's whole purpose is that a
 * request and its outcome differ -- ask for shared storage with no directory, or MatrixObject on a
 * build without it, and the engine starts a deployment on something else without erroring. A page
 * that renders the request back is indistinguishable, in source, from one that renders what the
 * plan resolved to. Only running it and reading the DOM separates them.
 *
 * Element identity is kept per id, and `change` events are dispatched through the real listeners,
 * so the select-driven reveal of the shared-directory field is exercised rather than assumed.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const routes = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

const posted = [];
const byId = new Map();

function makeEl(id) {
  const listeners = {};
  const attrs = {};
  const el = {
    id: id || "",
    hidden: false, innerHTML: "", textContent: "", value: "", checked: false, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
    removeEventListener() {},
    dispatch(type, ev) { (listeners[type] || []).forEach((fn) => fn(ev || {})); },
    appendChild() {}, removeChild() {},
    setAttribute(k, v) { attrs[k] = v; }, getAttribute(k) { return k in attrs ? attrs[k] : null; },
    closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
  return el;
}

function get(id) {
  if (!byId.has(id)) { byId.set(id, makeEl(id)); }
  return byId.get(id);
}

function respond(body) {
  return Promise.resolve({
    ok: true, status: 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body))
  });
}

const win = {
  document: {
