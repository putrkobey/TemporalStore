/* Run the shipped probeHtml and read what it says about the model that answered.
 *
 * A call that succeeded with the WRONG model is the failure this panel is least able to show: it
 * answered, it was fast, and the vectors are unusable. So the function is run rather than read.
 *
 * Usage: node probe_model_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("function probeHtml(r)");
if (start < 0) { console.log("probeHtml is not on this page"); process.exit(2); }
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const probeHtml = new Function("esc", page.slice(start, end) + "; return probeHtml;")(esc);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const base = { target: "embedding", ok: true, latency_ms: 12, url: "http://127.0.0.1:8400/v1" };

const agreed = probeHtml(Object.assign({}, base, {
  response: { model: "all-MiniLM-L6-v2", dimensions: 384,
              requested_model: "all-MiniLM-L6-v2", model_matches_request: true },
}));
ok("a model that matches reads as ok", />ok</.test(agreed), agreed);
ok("and is not styled as a problem", !/class="probe bad"/.test(agreed), agreed);

const differs = probeHtml(Object.assign({}, base, {
  response: { model: "all-MiniLM-L6-v2", dimensions: 384,
              requested_model: "text-embedding-3-small", model_matches_request: false },
}));
ok("a different model is not reported as ok", !/>ok</.test(differs), differs);
