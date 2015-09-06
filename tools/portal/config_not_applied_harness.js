/* Run the shipped renderSummary and read what it says about a configuration that is not in force.
 *
 * The distinction cannot be read off the source. A deployment with a corrupt config file and one
 * that has never been configured produce the SAME settings document -- `load()` returns an empty
 * one for both, on purpose, because the deployment has to start either way. So the only thing that
 * separates them on the screen is this banner, and the only way to know it is drawn is to draw it.
 *
 * Usage: node config_not_applied_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("function renderSummary(d) {");
if (start < 0) { console.log("renderSummary is not on this page"); process.exit(2); }
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}

const els = {};
function el(id) {
  if (!els[id]) { els[id] = { innerHTML: "", textContent: "" }; }
  return els[id];
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const scope = { $: el, esc, fields: {} };
const names = Object.keys(scope);
const renderSummary = new Function(...names,
  page.slice(start, end) + "; return renderSummary;")(...names.map((k) => scope[k]));

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function warningsFor(status) {
  els.warnings = { innerHTML: "", textContent: "" };
  renderSummary({ settings: { config_file_status: status }, warnings: [], extraction: {},
                  embedding: {} });
  return els.warnings.innerHTML;
