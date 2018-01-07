/* Run the shared "why was this refused" helper against the shapes the edge actually answers with.
 *
 * The interesting cases are not readable from the source: which half wins when both `detail` and
 * `error` are present, and whether the scope list is appended to that or quietly replaces it. The
 * edge sends `detail` for a rejected value ("extraction.provider must be one of ...") and
 * `required` for a refused scope, and a message that shows one and drops the other is how an
 * operator ends up guessing at something the response already answered.
 *
 * Usage: node refusal_message_harness.js <page.html> [more pages...]
 */
"use strict";
const fs = require("fs");
const path = require("path");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* Read out of a built page rather than the generator: this is the copy a browser runs. */
function helperFrom(file) {
  const page = fs.readFileSync(file, "utf8");
  const marker = "window.__matrixarkWhy = function (body, fallback) {";
  const start = page.indexOf(marker);
  if (start < 0) { return null; }
  const end = page.indexOf("\n  };", start);
