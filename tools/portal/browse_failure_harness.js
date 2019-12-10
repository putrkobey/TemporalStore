/* Run the Explore page's own loadBrowse with one of its two fetches failing, and read both panels.
 *
 * The two lists are loaded side by side by one function. When the subject list failed it was
 * BLANKED -- and an empty scope on this page has words ("No subjects hold memories in this scope
 * yet"), so a blank panel was a third state saying nothing at all. With the memory list succeeding
 * the screen contradicted itself: memories listed, and no subject holding any.
 *
 * `innerHTML = ""` and `innerHTML = "<div class=empty>...</div>"` are the same shape of statement,
 * so a diff cannot tell them apart. The shipped function is sliced out and run.
 *
 * Usage: node browse_failure_harness.js <explore_portal.html> [users|memories|both|none|empty]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const failing = process.argv[3] || "users";

/* The balanced-brace slice that takes loadBrowse out of the page. */
function sliceBlock(from) {
  let depth = 0;
  for (let i = page.indexOf("{", from); i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) { return page.slice(from, i + 1); } }
  }
  return null;
}

const start = page.indexOf("function loadBrowse() {");
if (start < 0) { console.log("FAIL loadBrowse is not on this page"); process.exit(2); }
const loadBrowseSrc = sliceBlock(start);

/* The page's shared helpers are a script block of their own, emitted before the page's own
 * script. The sandbox below models what the browse region can see, so it needs them too --
