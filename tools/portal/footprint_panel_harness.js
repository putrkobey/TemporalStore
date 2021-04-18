/* Run the Setup page, hand it a footprint, and read what it drew.
 *
 * Nothing here is provable from source. The renderer exists whether or not anything calls it, a
 * figure written into the wrong cell looks the same in a diff as one written into the right one,
 * and the case that matters most -- an engine that publishes no footprint -- differs from the
 * healthy case only in whether a dash or a zero reaches the page.
 *
 * Usage: node footprint_panel_harness.js <setup_portal.html> [published|absent|multiworker]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const mode = process.argv[3] || "published";
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const WORKER = {
  resident_bytes: 412 * 1048576,
  peak_bytes: 455 * 1048576,
  source: "/proc/self/status",
  workers: mode === "multiworker" ? 4 : 1
};
const ENGINE = mode === "absent" ? { available: false } : {
  available: true,
  cache_memory_bytes: 96 * 1048576,
  cache_disk_bytes: 12 * 1048576,
  cache_compression_saved_bytes: 3 * 1048576,
  store_logical_bytes: 6 * 1073741824,
  store_physical_bytes: 2 * 1073741824,
  store_compression_ratio: 3.0
};
const OVERVIEW = { status: "ok", footprint: { worker: WORKER, engine: ENGINE } };

const els = new Map();
function makeEl(id) {
  return {
    id: id || "", hidden: false, textContent: "",
    value: id === "key" ? "k-admin" : "", checked: true, className: "",
    style: {}, dataset: {}, files: [], options: [], children: [], _html: "",
    get innerHTML() { return this._html; },
    set innerHTML(v) { this._html = String(v); },
    addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
