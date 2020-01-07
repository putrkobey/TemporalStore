/* Run the API page and check each route row reports what that route has served.
 *
 * The page builds its rows as an HTML string, so a stub whose querySelectorAll returns nothing
 * would let every assertion pass without a single cell being painted. The routes container parses
 * what the page assigns to it and hands back real cells, which is the only way the painting is
 * actually exercised.
 *
 * Usage: node api_traffic_harness.js <api_portal.html>
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

/* Two documented routes sharing one counter, and one with a counter of its own. */
const ROUTES = [
  { group: "Memory", method: "GET", path: "/v1/memories", summary: "List memories.",
    scope: "read", metric: "/v1/memories" },
  { group: "Memory", method: "GET", path: "/v1/memory/{id}", summary: "One memory.",
    scope: "read", metric: "/v1/memory/{id}",
    metric_shared_with: ["/v1/memory/{id}/history"] },
  { group: "Memory", method: "GET", path: "/v1/memory/{id}/history", summary: "Its history.",
    scope: "read", metric: "/v1/memory/{id}",
    metric_shared_with: ["/v1/memory/{id}"] },
  { group: "Admin", method: "GET", path: "/v1/admin/scopes", summary: "Scopes.",
    scope: "admin", metric: "/v1/admin/scopes" },
];

const FRAME = {
  ts: 1, imports: {}, warnings: 0, embedding: { total: 1 },
  traffic: {
