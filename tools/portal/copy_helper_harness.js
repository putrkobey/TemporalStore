/* Run the shared copy helper, and one page's copy button, under a clipboard that misbehaves.
 *
 * The three outcomes cannot be told apart by reading: a resolved write, a rejected one (a denied
 * permission, an unfocused document), and no clipboard API at all -- which is what an http://
 * origin gives you, and what a self-hosted portal often is. Every caller used to announce success
 * for all three.
 *
 * Usage: node copy_helper_harness.js <page.html> [more pages...]
 */
"use strict";
const fs = require("fs");
const path = require("path");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* The helper is injected into every page by the builder, so it is read back out of a built page
   rather than out of the generator -- that is the copy a browser actually runs. */
function helperFrom(file) {
  const page = fs.readFileSync(file, "utf8");
  const marker = "window.__matrixarkCopyText = function (text) {";
  const start = page.indexOf(marker);
  if (start < 0) { return null; }
  const end = page.indexOf("\n  };", start);
  return page.slice(start, end + 5);
}

