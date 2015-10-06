/* Run the one-box profile and cap panels against what the endpoint actually returns.
 *
 * Both panels used to render from the settings registry -- they asked whether a knob by a given
 * name was offered, and drew the ones that were. That is not the question either heading asks.
 * When a later change stopped offering the six settings they named, both panels said "This build
 * offers none of them." while the profile went on deciding every result and the caps went on
 * cutting every retrieve. Nothing failed: a panel rendering an empty-state is a panel working.
 *
 * So the payload here is not written out by hand. It is produced by calling the gateway's own
 * effective_retrieval() and handed in as JSON, which means these assertions are made against the
 * shape the endpoint really serves. A renderer and a hand-written fixture agree with each other
 * forever, including after the endpoint stops agreeing with both.
 *
 * Usage: node onebox_effective_harness.js <onebox_portal.html> <payload.json>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const live = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function extract(marker) {
  const start = page.indexOf(marker);
  if (start < 0) { throw new Error("not on this page: " + marker); }
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") { depth += 1; }
    else if (page[i] === "}") { depth -= 1; if (depth === 0) { return page.slice(start, i + 1); } }
  }
  throw new Error("unbalanced: " + marker);
}

/* The page's own escaper, not a stand-in: the renderers call it on every value, so a harness with
   its own would be testing text the page never produces. */
const sandbox = {};
const source = [
  extract("function esc(s)"),
  /* Both cap and policy rows go through one level map now, so it has to come along or every row
     throws on an undefined helper. Extracted rather than restated: a harness with its own copy of
     the labels would agree with itself forever. */
  extract("var LEVEL = {"),
  extract("function levelBadge(source)"),
  extract("function unreadable(data, what)"),
  extract("function renderProfile(data)"),
  extract("function renderCaps(data)"),
  extract("function subjectLine(data)"),
  extract("function retrieveCounts(text)"),
  extract("function workerCount(text)"),
  extract("function renderAnswering(text)"),
  extract("function renderPolicy(knobs)"),
  "sandbox.renderProfile = renderProfile;",
  "sandbox.renderCaps = renderCaps;",
  "sandbox.renderPolicy = renderPolicy;",
  "sandbox.subjectLine = subjectLine;",
  "sandbox.renderAnswering = renderAnswering;",
  "sandbox.retrieveCounts = retrieveCounts;"
].join("\n");
new Function("sandbox", source)(sandbox);

/* ---------- what the endpoint really returned ---------- */
const profile = sandbox.renderProfile(live);
const caps = sandbox.renderCaps(live);

ok("the profile panel is not an empty state",
   !/offers none of them|Enter an admin key/.test(profile), profile.slice(0, 200));
ok("the cap panel is not an empty state",
   !/offers none of them|Enter an admin key/.test(caps), caps.slice(0, 200));

/* Every cap the endpoint reported reaches the table, with the value that is in force. A panel
   that renders three rows of the wrong number looks exactly like one that renders three rows. */
for (const cap of live.caps) {
  ok("cap " + cap.name + " is named", caps.indexOf(cap.name) >= 0);
  ok("cap " + cap.name + " shows the value in force (" + cap.value + ")",
     caps.indexOf(">" + cap.value + "<") >= 0,
     "looked for >" + cap.value + "< in the rendered table");
  ok("cap " + cap.name + " shows the build default (" + cap.build_default + ")",
     caps.indexOf(">" + cap.build_default + "<") >= 0);
  ok("cap " + cap.name + " names its variable", caps.indexOf(cap.env) >= 0);
  ok("cap " + cap.name + " says which level supplied it",
     ["tenant", "environment", "default"].indexOf(cap.source) >= 0,
