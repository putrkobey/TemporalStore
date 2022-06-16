// Runs the SHIPPED exchange panel and call log from the generated page.
// The renderers are what a person debugging reads, so what matters is the HTML they produce.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

function fn(name) {
  const start = page.indexOf("function " + name + "(");
  if (start < 0) throw new Error("not found: " + name);
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + name);
}

const nodes = {};
const el = (id) => (nodes[id] = nodes[id] || { innerHTML: "" });

