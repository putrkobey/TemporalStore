/* Feed the shared strip a frame and read what the "awaiting restart" segment did.
 *
 * The strip's render() is wrapped by its caller in a try/catch that ignores errors, so a segment
 * that throws is a segment that silently does nothing -- and that block defines only n, show and
 * plural, so reaching for any other helper fails exactly that way and looks identical to a healthy
 * deployment with nothing waiting. Reading the source cannot tell those apart.
 *
 * Usage: node waiting_segment_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const els = new Map();
function makeEl(id) {
  return {
    /* The same shape the datanode segment's harness uses: the pages read an admin key out of a
       field and trim it, so a stub without `value` throws at load and every check below then
       reports on a strip that never ran. */
    id: id || "", hidden: true, innerHTML: "", textContent: "", className: "",
    value: id === "key" ? "k-admin" : "", checked: false,
