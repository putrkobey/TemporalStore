/* An empty audit table has to say WHICH kind of empty it is.
 *
 * MATRIXARK_AUDIT_MODE defaults to off, so on most deployments an empty audit log means nothing
 * was KEPT -- not that nothing happened. A panel that renders "no records" over both is at its
 * most reassuring exactly when it should not be: every refusal was discarded and the page implies
 * there were none.
 *
 * The endpoint reports the recording mode beside the rows for this reason, and whether the panel
 * uses it is behaviour, not markup. So the page's own three functions are sliced out and run
 * against canned payloads: the network is stubbed because it is the boundary, and everything that
 * decides what the reader sees comes from the page.
 *
 * Usage: node audit_panel_harness.js <api_key_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(name) {
  const at = page.indexOf("function " + name + "(");
  if (at === -1) { return null; }
  let depth = 0;
