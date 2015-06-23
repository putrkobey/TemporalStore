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
 * and it RUNS the shipped block rather than stubbing it, because a stub would let the helper
 * change without a single test noticing. That block is where `__matrixarkWhen` is defined, which
 * is what loadBrowse formats each row's timestamp through -- so running it covers the formatter
 * and the three helpers beside it, and there is exactly one `win` for all of them. */
const sharedAt = page.indexOf("Helpers every page may call");
if (sharedAt < 0) { console.log("FAIL the shared helper block is not on this page"); process.exit(2); }
const sharedFrom = page.lastIndexOf("<script>", sharedAt) + "<script>".length;
const sharedTo = page.indexOf("</script>", sharedFrom);
const win = {};
new Function("window", page.slice(sharedFrom, sharedTo))(win);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const els = {};
function el(id) {
  if (!els[id]) { els[id] = { id: id, innerHTML: "", textContent: "" }; }
  return els[id];
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const said = [];
function respond(body, good) {
  return Promise.resolve({
    ok: good, status: good ? 200 : 500,
    json: () => Promise.resolve(body)
  });
}

const scope = {
