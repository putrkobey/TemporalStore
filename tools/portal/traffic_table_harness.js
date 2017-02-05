/* The traffic panel is drawn from two sources. They must draw the same table.
 *
 * At load the Setup page reads /v1/metrics and parses the Prometheus text -- that needs no key, so
 * it works before the reader has authenticated. A second later the first live frame arrives and
 * redraws the same element from the metrics snapshot.
 *
 * They used to build different tables: four columns from the scrape, six from the frame. The panel
 * changed shape under the reader on every load, and a deployment whose stream never connects stayed
 * on the four-column one -- with no way to tell that a tail statistic existed at all.
 *
 * Two builders producing "the same" table is not something source reading settles, so both are run
 * over equivalent inputs and their output compared.
 *
 * Usage: node traffic_table_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(name) {
  const at = page.indexOf("function " + name + "(");
  if (at === -1) { return null; }
  let depth = 0;
  for (let i = page.indexOf("{", at); i < page.length; i += 1) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") {
      depth -= 1;
      if (depth === 0) { return page.slice(at, i + 1); }
    }
  }
  return null;
}

const parts = ["statusChips", "trafficTable", "renderTraffic", "renderLiveTraffic"];
const sliced = {};
for (const name of parts) {
  sliced[name] = slice(name);
  ok("the page defines " + name, !!sliced[name]);
}
if (parts.some((n) => !sliced[n])) { console.log("FAILED " + failures); process.exit(1); }

ok("both renderers go through one builder",
   /trafficTable\(/.test(sliced.renderTraffic) && /trafficTable\(/.test(sliced.renderLiveTraffic),
   "two builders is how the two views came to disagree");

function run(which, argument) {
  const written = { html: "" };
  const env = {
    $: () => ({ set innerHTML(v) { written.html = v; }, get innerHTML() { return written.html; } }),
    esc: (s) => String(s == null ? "" : s),
  };
  const src = sliced.statusChips + "\n" + sliced.trafficTable + "\n" + sliced[which]
    + "\n; return " + which + ";";
  new Function(...Object.keys(env), src)(...Object.values(env))(argument);
  return written.html;
}

function headers(html) {
  const row = /<thead><tr>([\s\S]*?)<\/tr>/.exec(html);
