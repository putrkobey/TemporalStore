/* Apply a pasted configuration and read what the page said afterwards.
 *
 * An export omits secret values on purpose -- blanking them at the target would clear a working
 * key -- and the export button says so to the person exporting. The file does not carry that note,
 * so the person applying it somewhere else is told "Applied 47 settings." and nothing more. Whether
 * the missing credentials are mentioned depends on a reload finishing and a second message
 * replacing the first, which is behaviour a reader of the source cannot confirm.
 *
 * Usage: node import_secrets_harness.js <setup_portal.html>
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

const els = new Map();
const listeners = {};
function makeEl(id) {
  return {
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false, disabled: false,
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener(type, fn) { listeners[(id || "") + ":" + type] = fn; },
    removeEventListener() {}, appendChild() {}, removeChild() {},
    setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    scrollIntoView() {}, focus() {}, click() {},
    classList: { add() {}, remove() {}, toggle() {} }
  };
}
function el(id) {
  if (!els.has(id)) { els.set(id, makeEl(id)); }
  return els.get(id);
}

/* One secret configured, one not: the message must name the second and only the second. */
function snapshot() {
  return {
    status: "ok",
    settings: {
      status: "ok", groups: { Extraction: [
        { key: "extraction.model", kind: "str", label: "Model", help: "", value: "m",
          env: "MATRIXARK_EXTRACTION_MODEL", applies: "live", source: "config",
          essential: false, default: "", overridable_by: [], boot_pinned: false,
          pending_restart: false },
        { key: "extraction.api_key", kind: "secret", label: "API key", help: "", value: null,
          configured: false, env: "MATRIXARK_EXTRACTION_API_KEY", applies: "restart",
          source: "default", essential: false, default: "", overridable_by: [],
          boot_pinned: false, pending_restart: false },
        { key: "embedding.api_key", kind: "secret", label: "Embedding key", help: "", value: null,
          configured: true, env: "MATRIXARK_EMBEDDING_API_KEY", applies: "restart",
          source: "config", essential: false, default: "", overridable_by: [],
          boot_pinned: false, pending_restart: false }
      ] },
      group_meta: {}, presets: [], history: [], inventory: {}, essential_keys: [],
      config_file: "/tmp/runtime.json", pending_restart: []
    },
    warnings: []
  };
}

