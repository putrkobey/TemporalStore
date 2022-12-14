/* Run the Setup page, hand it a traffic series, and read the chart it drew.
 *
 * A chart is the easiest panel to make lie. An empty one and a quiet one look the same; a missing
 * sample drawn as zero is a dip that never happened; a series too short to plot rendered as a flat
 * line reads as "no traffic" rather than "not watching long enough". None of that is visible in a
 * diff, so the page is run and the SVG is read.
 *
 * Usage: node trend_panel_harness.js <setup_portal.html> [full|short|absent|gaps|noresident]
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const mode = process.argv[3] || "full";
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function points(n) {
  const out = [];
  for (let i = 0; i < n; i += 1) {
    out.push({
      at: 1788600000 + i * 15,
      requests_per_s: 1 + (i % 4),
      errors_per_s: i === 3 ? 2 : 0,
      /* An interval in which nothing completed has no mean. Drawing it as 0 would show a latency
         dip that never happened. */
      mean_ms: (mode === "gaps" && i % 2 === 1) ? null : 12 + i,
      /* A LEVEL, not a rate. 100 MiB climbing by 1 MiB an interval: if anyone differences it the
         peak reads about 1 instead of about 107, which is the whole difference between plotting
         a footprint and plotting its change. `noresident` is the platform that cannot report a
         size -- every point null, and the chart must be absent rather than drawn along zero. */
      resident_bytes: mode === "noresident" ? null : (100 + i) * 1048576,
    });
  }
  return out;
}

const SERIES = mode === "absent" ? undefined : {
  interval_s: 15,
  points: mode === "short" ? points(1) : points(8),
