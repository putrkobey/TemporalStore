/* Run the overview page, give it a frame, press copy, and read the bundle it assembled.
 *
 * The bundle is built in a closure from a live frame plus three fetches. Whether a field reaches
 * it cannot be read off the source: the frame half only arrives if the page's own stream delivers,
 * and a field referencing something the page never stored would come out null while the source
 * looks correct.
 *
 * Usage: node bundle_harness.js <overview_portal.html>
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

const FRAME = {
  ts: 1, warnings: 0, imports: {}, embedding: { total: 3 },
  datanode: "unreachable",
  traffic: {
    total_requests: 5, total_errors: 2, in_flight: 0,
    routes: { "/v1/retrieve": { requests: 5, errors: 2, avg_ms: 9, p95_ms: 25,
                                statuses: { "200": 3, "503": 2 } } },
    recent_failures: [
      { at: 1730000000, route: "/v1/retrieve", method: "POST", status: 503 },
      { at: 1729999990, route: "/v1/retrieve", method: "POST", status: 503 }
    ]
  }
};
const SSE = "event: status\ndata: " + JSON.stringify(FRAME) + "\n\n";

const clicks = {};
let copied = null;

