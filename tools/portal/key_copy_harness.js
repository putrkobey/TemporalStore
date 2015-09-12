/* Run the key portal's copy paths and read what they actually did.
 *
 * Three things here cannot be seen by reading the source. Whether a copy button is offered when
 * the server answers without a key. What lands on the clipboard when it is clicked -- the secret
 * alone, or the secret with the id and the warning swept up around it. And what the button says
 * when the clipboard REFUSES, which is the case the previous code got wrong: it reported "Copied"
 * unconditionally, and for a key shown exactly once that is the difference between a copied key
 * and a lost one.
 *
 * Usage: node key_copy_harness.js <api_key_portal.html> [mutation]
 *   mutation = nogate   -- offer the button even with no key  (case B must go red)
 *   mutation = oldcopy  -- report "Copied" unconditionally     (cases C, D, F must go red)
 */
"use strict";
const fs = require("fs");

const PATH = process.argv[2];
const MUTATION = process.argv[3] || "";
/* Deliberately unlike a real key: low-entropy, no live-looking prefix, and named for what it
   is. A fixture shaped like a credential trains the secret scanner's readers to wave findings
   through, and the alternative -- allowlisting this file by path -- would hide a real one later. */
const CREATED_KEY = "created-key-placeholder-not-a-real-key";
const ROTATED_KEY = "rotated-key-placeholder-not-a-real-key";

const GATE = "(secret ? \" <button type='button' class='mini' id='copyNewKey'>Copy key</button>\" : \"\") +";
const UNGATED = "\" <button type='button' class='mini' id='copyNewKey'>Copy key</button>\" +";
const HONEST = "      button.textContent = ok ? done : \"Copy failed\";";
const ALWAYS = "      button.textContent = done;";

let source = fs.readFileSync(PATH, "utf8");
if (MUTATION) {
  const before = source;
  if (MUTATION === "nogate") { source = source.replace(GATE, UNGATED); }
  else if (MUTATION === "oldcopy") { source = source.replace(HONEST, ALWAYS); }
  else { console.log("FAIL unknown mutation " + MUTATION); process.exit(1); }
  if (source === before) {
    console.log("FAIL mutation '" + MUTATION + "' matched nothing -- it tests neither code nor test");
    process.exit(1);
  }
}

const scripts = [...source.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---- one page, with a clipboard that behaves as the case requires ---------------------------- */
function boot(clipboard) {
  const els = new Map();
  const created = [];
  const timers = [];
  const asked = [];              // every window.confirm message, in order
  let answer = true;             // what the person clicks in the dialog

  function makeEl(id) {
    return {
      id: id || "", hidden: false, innerHTML: "", textContent: "", value: "", className: "",
      disabled: false, checked: false, onclick: null, style: {}, dataset: {}, children: [],
      _listeners: {},
      addEventListener(t, fn) { (this._listeners[t] = this._listeners[t] || []).push(fn); },
      dispatch(t, ev) { (this._listeners[t] || []).forEach((fn) => fn(ev || {})); },
      removeEventListener() {},
      appendChild(child) { this.children.push(child); }, removeChild() {}, remove() {},
      setAttribute() {}, getAttribute() { return null; },
      closest() { return null; },
      /* The key table's renderer reaches for a tbody and then for the row's last cell, and both
         have to be real objects or the rows -- and the Rotate button on them -- never exist. */
      querySelector(sel) {
        this._found = this._found || {};
        if (!this._found[sel]) { this._found[sel] = makeEl((id || "") + " " + sel); }
        return this._found[sel];
      },
      get lastChild() {
        if (!this._last) { this._last = makeEl((id || "") + ":last"); }
        return this._last;
      },
      querySelectorAll() { return []; },
      scrollIntoView() {}, focus() {}, click() { if (this.onclick) { this.onclick({}); } },
      classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } }
    };
  }
  function el(id) {
    if (!els.has(id)) { els.set(id, makeEl(id)); }
    return els.get(id);
  }

  function reply(body) {
    return Promise.resolve({
      ok: true, status: 200,
      text: () => Promise.resolve(JSON.stringify(body)),
      json: () => Promise.resolve(body)
    });
  }

  let createResponse = { api_key_id: "ak_new", api_key: CREATED_KEY };
  let createStatus = 200;
  const rotateResponse = { api_key_id: "ak_rotated", api_key: ROTATED_KEY };

  const store = { mtx_admin_key: "admin-key-for-the-harness" };
  const storage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem(k, v) { store[k] = String(v); },
    removeItem(k) { delete store[k]; }
  };

  const win = {
    document: {
      getElementById: (id) => el(id),
      querySelector: () => null,
      querySelectorAll: () => [],
      createElement() { const e = makeEl(); created.push(e); return e; },
      createTextNode() { return makeEl(); },
      addEventListener() {}, body: makeEl(), hidden: false
    },
    addEventListener() {},
    location: { origin: "http://localhost", href: "http://localhost", reload() {} },
    localStorage: storage, sessionStorage: storage,
    navigator: clipboard === null ? {} : { clipboard: clipboard },
    /* Recorded, never fired: the label restores after 1.4s, and firing that would erase the very
       thing each case asserts. That the restore was scheduled is checked on its own. */
    setTimeout(fn) { timers.push(fn); return timers.length; },
    setInterval: () => 0, clearInterval() {}, clearTimeout() {},
    wireTabs() {}, requestAnimationFrame: () => 0,
    confirm(message) { asked.push(String(message)); return answer; },
    Number, JSON, String, Math, Date, Object, Array, Promise, RegExp, Error, isNaN,
    encodeURIComponent, decodeURIComponent, parseInt, parseFloat, console,
    TextDecoder: function () { this.decode = () => ""; },
    AbortController: function () { this.abort = () => {}; this.signal = {}; },
    FileReader: function () {}, Blob: function () {},
    URL: { createObjectURL: () => "", revokeObjectURL() {} },
    fetch: (url) => {
      const u = String(url);
      if (u.indexOf("create_api_key") >= 0) {
        if (createStatus !== 200) {
          const body = JSON.stringify(createResponse);
          return Promise.resolve({
            ok: false, status: createStatus,
            text: () => Promise.resolve(body), json: () => Promise.resolve(createResponse)
          });
        }
        return reply({ result: createResponse });
      }
      if (u.indexOf("rotate_api_key") >= 0) { return reply({ result: rotateResponse }); }
      if (u.indexOf("list_api_keys") >= 0) {
        /* The field names are the served ones: the list reads `api_keys`, and only a row whose
           status is exactly "active" gets a Rotate button built for it. A fixture shaped to a
           guess would render nothing and let the rotate case pass having tested nothing. */
        return reply({ result: { api_keys: [{
          api_key_id: "ak_new", tenant_id: "t1", account_id: "a1", role: "service",
          scopes: ["read", "write"], status: "active", request_quota: 0, quota_window: 0,
          created_at_ms: 1756000000000, last_used_at_ms: 1756000090000, usage_count: 3
        }] } });
      }
      return new Promise(() => {});
    }
  };
  win.window = win;

  const names = Object.keys(win);
  const values = names.map((k) => win[k]);
  scripts.forEach((body, index) => {
    try { new Function(...names, body)(...values); }
    catch (e) { console.log("FAIL script " + index + " threw: " + e.message); failures += 1; }
  });

  return {
    el, created, timers, asked,
    answerDialogs(value) { answer = value; },
    setCreateResponse(v) { createResponse = v; },
    setCreateStatus(v) { createStatus = v; },
    create() {
      /* Both are required, and createKey returns quietly without either -- which would leave the
         output empty and let an "is no button offered?" check pass having rendered nothing. */
      el("ckScopes").value = "read";
      el("ckTenant").value = "t1";
      const btn = el("createBtn");
