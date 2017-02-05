/* Run the catalogue page's Skills/Resources tabs.
 *
 * One filter covers both lists while only one is visible, so the interesting behaviour is what
 * happens when the query matches the tab you are NOT looking at. Reading the source cannot tell a
 * page that says so from one that shows an empty list.
 *
 * Usage: node catalog_tabs_harness.js <catalog_portal.html>
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

const SKILLS = { skills: [
  { name: "deploy-runbook", description: "How to deploy the airship", updated_at: 1 },
  { name: "oncall", description: "Who to wake", updated_at: 2 },
] };
const RESOURCES = { resources: [
  { name: "hull-spec.md", description: "Airship hull specification", updated_at: 3 },
] };

const listeners = {};
const els = new Map();

function attr(tag, name) {
  const m = tag.match(new RegExp(name + '="([^"]*)"'));
  return m ? m[1] : null;
}
