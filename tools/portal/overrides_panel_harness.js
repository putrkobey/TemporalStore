/* Run the key portal's overrides renderer against real payload shapes.
 *
 * The page loading without throwing proves nothing about this panel: renderOverrides only runs on
 * a button press, so a call to a helper that does not exist -- which is exactly the bug this
 * harness was written after -- loads clean and fails when someone clicks.
 *
 * The renderer touches one table body, so it gets a stub that remembers what was written rather
 * than a full DOM.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]).join("\n");

function extract(name) {
  const start = scripts.indexOf("function " + name + "(");
  if (start < 0) { return null; }
  let depth = 0;
  for (let i = scripts.indexOf("{", start); i < scripts.length; i += 1) {
    if (scripts[i] === "{") { depth += 1; }
    else if (scripts[i] === "}") {
      depth -= 1;
      if (depth === 0) { return scripts.slice(start, i + 1); }
    }
  }
  return null;
}

const renderSrc = extract("renderOverrides");
const escapeSrc = extract("escapeHtml");
if (!renderSrc) { console.log("FAIL renderOverrides is not in the page"); process.exit(1); }
if (!escapeSrc) { console.log("FAIL escapeHtml is not in the page"); process.exit(1); }

let failures = 0;
const body = { innerHTML: "", appendChild(node) { this.innerHTML += node.innerHTML; } };
const table = { querySelector: () => body };
const doc = { createElement: () => ({ innerHTML: "" }) };
const factory = new Function(
  "$", "document",
  escapeSrc + "\n" + renderSrc + "\n; return renderOverrides;");
const renderOverrides = factory(() => table, doc);

function run(label, payload, checks) {
  body.innerHTML = "";
  try {
    renderOverrides(payload);
  } catch (err) {
