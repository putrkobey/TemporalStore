// Runs the SHIPPED loadUsage against a DOM stub. Reading the source would pass on a function that
// builds the sentence and never shows it.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

const start = page.indexOf("  function loadUsage() {");
if (start < 0) throw new Error("loadUsage not found");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
