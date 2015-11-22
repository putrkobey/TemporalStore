/* One explanation per gateway status, and the page-specific words that survive.
 *
 * Three pages carried a status map of their own and each carried a different subset. Ingestion --
 * the page that uploads files -- was the one with no sentence for 413, so an upload refused for
 * size read as "The gateway answered 413." on the panel where the cause is obvious to everyone
 * except the reader. Four pages had no map at all.
 *
 * A map is a data structure and looks fine in source whatever it omits, so the page's own helper
 * is executed here and asked about each status.
 *
 * Usage: node status_message_harness.js <page.html> [...]
 */
"use strict";
const fs = require("fs");
const path = require("path");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(page, header) {
  const at = page.indexOf(header);
  if (at === -1) { return null; }
  let depth = 0;
  for (let i = page.indexOf("{", at); i < page.length; i += 1) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") {
      depth -= 1;
      if (depth === 0) { return page.slice(at, i + 1); }
    }
  }
  return null;
}

/* Statuses the edge actually answers with, and what a reader must not be left holding. */
const EXPLAINED = [401, 403, 413, 429, 502, 503, 504];

const files = process.argv.slice(2);
ok("pages were given", files.length > 0, "none");

let withOwn = 0;
for (const file of files) {
  const name = path.basename(file).replace("_portal.html", "");
  const page = fs.readFileSync(file, "utf8");

  const shared = slice(page, "window.__matrixarkFailure = function (status, overrides) {");
  ok(name + ": carries the shared status map", !!shared);
  if (!shared) { continue; }

  const win = {};
  new Function("window", shared + ";")(win);
  const say = win.__matrixarkFailure;

  const bare = EXPLAINED.filter((s) => /^The gateway answered/.test(say(s)));
  ok(name + ": every status the edge answers with has a sentence", bare.length === 0,
