/* Run the API page's group tabs and read what they did.
 *
 * Two things here cannot be seen in source. The list is rebuilt on every keystroke, so whether the
 * open tab survives a re-render is behaviour. And the text filter searches every group while only
 * one pane is visible -- if it quietly searched the open tab alone, a reference page would answer
 * "nothing matches that" while holding the match one tab away.
 *
 * Usage: node api_tabs_harness.js <api_portal.html>
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

const ROUTES = [
  { group: "Memory", method: "POST", path: "/v1/ingest", summary: "Store a memory.",
    scope: "write", metric: "/v1/ingest" },
  { group: "Memory", method: "POST", path: "/v1/retrieve", summary: "Recall memories.",
    scope: "read", metric: "/v1/retrieve" },
  { group: "Administration", method: "GET", path: "/v1/admin/config",
    summary: "Read the configuration blueprint.", scope: "admin",
    metric: "/v1/admin/config" },
  { group: "Blobs", method: "PUT", path: "/v1/blob/{key}",
    summary: "Upload a zeppelin blueprint.", scope: "write", metric: "/v1/blob/{key}" },
];

let tabCells = [];
let panes = [];
const listeners = {};
const els = new Map();
