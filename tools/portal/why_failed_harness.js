/* Run the page's OWN failure classifier against the three things that reach a catch.
 *
 * The shared helper block is executed, not stubbed: the whole point of the change is what the
 * shipped classifier answers, and a copy of it here could drift from the page without a word.
 *
 * Usage: node why_failed_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

const sharedAt = page.indexOf("Helpers every page may call");
if (sharedAt < 0) { console.log(JSON.stringify({ error: "no shared helper block" })); process.exit(2); }
const from = page.lastIndexOf("<script>", sharedAt) + "<script>".length;
const to = page.indexOf("</script>", from);

const win = {};
