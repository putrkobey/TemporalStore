/* Does the status strip ask, or does it assume it is not allowed?
 *
 * On the developer default the gateway answers every admin route anonymously -- 200 with no
 * Authorization header at all -- and 401 once auth is on. The strip used to refuse to try:
 *
 *     var k = key();
 *     if (!k) { setTimeout(open, 2000); return; }
 *
 * so on the deployment somebody is most likely to be looking at first, the strip stayed dark on
 * all seven panels because the page would not ask a question the gateway was willing to answer.
 *
 * Both halves have to hold, and they pull against each other: ask when there is no key, and still
 * watch for a key on a deployment that wants one rather than backing off to thirty seconds. So the
 * page's own strip is run against a gateway that answers and a gateway that refuses.
 *
 * Usage: node open_deployment_harness.js <page.html>
 */
"use strict";
const fs = require("fs");
const realSetImmediate = setImmediate;

const page = fs.readFileSync(process.argv[2], "utf8");
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* The strip's own block, not the page's.
 *
 * Overview and Setup run a connection of their own and the strip stands down for them, and since
 * the page-level stream gained the same hidden-tab rule its comment matches too. The line that
 * only the strip has is the stand-down test itself. */
const src = scripts.find((s) => s.includes("if (window.__matrixarkLive) { return; }"));
if (!src) { console.log("FAIL the page carries no live strip"); process.exit(1); }

async function flush() {
  for (let i = 0; i < 8; i += 1) {
    await new Promise((resolve) => realSetImmediate(resolve));
  }
}

/* `answer` decides what the fake gateway does with each request. */
function world(answer, keyValue) {
  const timers = [];
  let now = 0;
  const calls = [];
  const listeners = { document: {}, window: {} };

  function element(id) {
    return {
