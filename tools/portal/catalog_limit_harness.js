// Runs the SHIPPED catalog `load` against stubs. Reading the source would pass on a page that
// computes the sentence and never shows it, which is the mistake this whole family of checks
// exists to catch.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

const start = page.indexOf("  function load(keepMessage) {");
if (start < 0) throw new Error("load not found");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
