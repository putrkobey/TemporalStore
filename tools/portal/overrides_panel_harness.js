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
