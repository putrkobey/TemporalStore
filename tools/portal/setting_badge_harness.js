/* Run the setup page's fieldHtml and read the badges it produces.
 *
 * Whether the two badges are mutually exclusive cannot be read off the source with any confidence:
 * "needs restart" describes the KIND of setting and is true from page load, and "changed — restart
 * to apply" describes THIS deployment right now. Showing both says the same thing twice and buries
 * the half that is about today, so the second has to replace the first rather than join it.
 *
 * Usage: node setting_badge_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

