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

/* Read out of the built page rather than the generator: this is the copy a browser runs. */
const start = page.indexOf("function fieldHtml(f) {");
if (start < 0) { console.log("FAIL fieldHtml is not on the page"); process.exit(1); }
const end = page.indexOf("\n  }", start);
const body = page.slice(start, end + 4);

const esc = (s) => String(s === undefined || s === null ? "" : s);

/* The renderer reaches for several small helpers from the page around it, and naming them one by
   one turns each into a round of "run it, read the ReferenceError, add a stub". Anything it asks
   for that is not a real global answers with an inert function instead: this is about which badges
   come out, and helper text would only be noise to match against. */
const scope = new Proxy({}, {
  has: () => true,
  get: (_target, key) => {
    if (key === Symbol.unscopables) { return undefined; }
    if (key === "esc") { return esc; }
    if (key in globalThis) { return globalThis[key]; }
    return () => "";
  }
