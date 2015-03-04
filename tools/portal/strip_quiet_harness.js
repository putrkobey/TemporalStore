// Drives the SHIPPED strip client from a generated page: opens, speaks, then goes quiet.
// The strip is an IIFE inside the shared nav script, so it is located by its content
// (`__matrixarkLive = "strip"`) rather than by a name, and evaluated with the globals it uses.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

// Find the IIFE whose body claims the strip.
function stripIife(text) {
  const marker = text.indexOf('__matrixarkLive = "strip"');
  if (marker < 0) throw new Error("strip client not found");
  let start = text.lastIndexOf("(function () {", marker);
  if (start < 0) throw new Error("no enclosing IIFE");
  let depth = 0;
  for (let i = text.indexOf("{", start); i < text.length; i++) {
    if (text[i] === "{") depth++;
    // The slice starts at the IIFE's opening paren, so close it: the body's brace alone
    // leaves  unbalanced.
    else if (text[i] === "}") { depth--; if (depth === 0) return text.slice(start, i + 1) + ")();"; }
  }
