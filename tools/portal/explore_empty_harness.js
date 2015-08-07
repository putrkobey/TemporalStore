/* Run the SHIPPED "why did this come back empty" sentence from the generated Explore page.
 *
 * Three causes look identical from the page's side, and naming the wrong one sends a reader to
 * check a setting that is already right -- which is worse than saying nothing, because they find
 * it right and conclude the page is broken.
 *
 * The third cause is shedding: under load the gateway answers with an empty pack in about 100 ms
 * and says so in `warnings`. Before this, the page rendered that warning as the backend's own
 * string and told the reader, in a full sentence above it, to go and look at their embedding
 * provider.
 *
 * Usage: node explore_empty_harness.js <explore_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function extract(marker) {
  const start = page.indexOf(marker);
  if (start < 0) { throw new Error("not on this page: " + marker); }
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  throw new Error("unbalanced: " + marker);
}

const sandbox = {};
new Function("sandbox", [
  extract("function wasShed(d)"),
