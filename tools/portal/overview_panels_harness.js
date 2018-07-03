/* Run the Setup page, hand it one overview response, and read what the three panels drew.
 *
 * The claim under test is not just that each panel renders. It is that ONE response feeds all
 * three: the overview already carried the footprint, the budget and the timing, and asking three
 * times for the same answer would be three times the probing. A fetch count is the only way to see
 * that from outside.
 *
 * Usage: node overview_panels_harness.js <setup_portal.html> [differ|aligned]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const mode = process.argv[3] || "differ";
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const HOOK_BUDGET = mode === "aligned" ? 500000 : 10000;
const OVERVIEW = {
  status: "ok",
  footprint: {
    worker: { resident_bytes: 412 * 1048576, peak_bytes: 455 * 1048576,
              source: "/proc/self/status", workers: 1 },
    engine: { available: false }
  },
  latency: mode === "aligned" ? {
    available: true, deadline_ms: 0, deadline_is_cooperative: true,
    transport_request_timeout_ms: 60000, transport_io_timeout_ms: 60000,
    bounds_a_slow_call: "transport_request_timeout_ms",
    worst_case_single_call_ms: 60000, deadline_can_be_overrun_by_ms: 0
  } : {
