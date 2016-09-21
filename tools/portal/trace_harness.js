/* Run the page's OWN call recorder against a fetch that answers, one that refuses, and one that
 * never answers at all.
 *
 * The shared block is executed rather than copied: the recorder replaces window.fetch, and the one
 * property that matters -- that it hands the response back untouched -- cannot be checked against
 * a copy of the code.
 *
 * Usage: node trace_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const at = page.indexOf("Helpers every page may call");
if (at < 0) { console.log(JSON.stringify({ error: "no shared helper block" })); process.exit(2); }
const from = page.lastIndexOf("<script>", at) + "<script>".length;
