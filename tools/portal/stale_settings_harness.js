/* Load the setup page against a deployment whose config file holds settings this build dropped.
 *
 * Whether the note appears, and whether it appears when there is nothing to say, is behaviour: the
 * warnings area is shared with the model-configuration warnings, and an addition that swallowed
 * those or that drew an empty note every time would read the same in the source.
 *
 * Usage: node stale_settings_harness.js <setup_portal.html> [--none]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const none = process.argv.indexOf("--none") >= 0;
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const els = new Map();
function makeEl(id) {
  return {
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false, disabled: false,
    style: {}, dataset: {}, files: [], options: [], children: [],
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
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

const STALE = none ? [] : ["embedding.old_name", "extraction.retired_knob"];

function snapshot() {
  return {
    status: "ok",
    /* A real warning alongside, so an addition that swallowed the existing ones would show. */
    warnings: ["extraction is falling back to rules"],
    extraction: { provider: "deterministic" }, embedding: { provider: "deterministic" },
    settings: {
      status: "ok",
      groups: { Extraction: [
        { key: "extraction.model", kind: "str", label: "Model", help: "", value: "m",
          env: "MATRIXARK_EXTRACTION_MODEL", applies: "live", source: "config", essential: false,
          default: "", overridable_by: [], boot_pinned: false, pending_restart: false }
      ] },
      group_meta: {}, presets: [], history: [], inventory: {}, essential_keys: [],
      config_file: "/etc/matrixark/runtime_config.json",
      pending_restart: [], unknown_stored: STALE
    }
  };
}

