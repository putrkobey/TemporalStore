/* When someone else changes the configuration, what does the Setup page do?
 *
 * Two answers, and picking the wrong one loses work. With nothing unsaved, taking their change is
 * right -- the form is showing stale values. With unsaved edits, reloading would discard what the
 * person here has typed, so it must say something instead and leave the edits alone.
 *
 * Both paths run through the same handler and differ only by a condition, which is exactly the
 * kind of thing that reads correct and behaves wrong. Frames are released on demand rather than on
 * a timer, because what is being waited on is a frame having been PROCESSED.
 *
 * Usage: node setup_config_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");
process.on("unhandledRejection", (e) => {
  console.log("  unhandled rejection: " + String(e && e.message ? e.message : e));
});

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const FIELD = { key: "extraction.provider", value: "deterministic", kind: "text",
                label: "Extraction provider",
                /* The page does `f.help.length` unguarded, so a field without help throws and the
                   error surfaces as "Could not reach the gateway". The server always sends it. */
                help: "Which provider extracts memories.", env: "MATRIXARK_EXTRACTION_PROVIDER",
                applies: "live", group: "models" };
const CONFIG = { settings: { groups: { models: [FIELD] },
                             group_meta: { models: { title: "Models", order: 1 } },
