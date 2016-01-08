/* Does the ingestion page notice when a stop is refused?
 *
 * fetch resolves for a 403 exactly as readily as for a 200 -- only a network failure rejects. A
 * handler that goes straight to .then(loadJobs) therefore reloads the table on a refusal and says
 * nothing, and the reader watching an import that is still running takes the stop for a slow one.
 *
 * That is invisible in source: the code reads as a completed action with a refresh at the end. So
 * the page's own cancelJob is run against canned responses and asked what it did.
 *
 * Usage: node stop_refusal_harness.js <ingestion_portal.html>
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
