/* Run the Setup page's byteHint against real settings shapes and report what it produced.
 *
 * Grepping the built page proves the function is present, not that it fires: a hint appended to a
 * variable nothing renders, or a pattern that never matches the variable names actually shipped,
 * both leave the same source text in the file. byteHint is pure, so it is extracted and called --
 * the thing under test is what it returns for a given field.
 *
 * Cases are the settings the portal really offers, including the two quota knobs that predate the
 * engine group, so a pattern matching only TS_* would be caught here.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]).join("\n");

const start = scripts.indexOf("function byteHint(");
if (start < 0) {
  console.log("FAIL byteHint is not in the built page");
  process.exit(1);
}
let depth = 0;
let end = -1;
for (let i = scripts.indexOf("{", start); i < scripts.length; i += 1) {
  if (scripts[i] === "{") {
    depth += 1;
  } else if (scripts[i] === "}") {
    depth -= 1;
    if (depth === 0) { end = i + 1; break; }
  }
}
if (end < 0) {
  console.log("FAIL could not brace-match byteHint");
  process.exit(1);
}
