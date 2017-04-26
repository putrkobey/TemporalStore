/* An empty audit table has to say WHICH kind of empty it is.
 *
 * MATRIXARK_AUDIT_MODE defaults to off, so on most deployments an empty audit log means nothing
 * was KEPT -- not that nothing happened. A panel that renders "no records" over both is at its
 * most reassuring exactly when it should not be: every refusal was discarded and the page implies
 * there were none.
 *
 * The endpoint reports the recording mode beside the rows for this reason, and whether the panel
 * uses it is behaviour, not markup. So the page's own three functions are sliced out and run
 * against canned payloads: the network is stubbed because it is the boundary, and everything that
 * decides what the reader sees comes from the page.
 *
 * Usage: node audit_panel_harness.js <api_key_portal.html>
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

const parts = ["sayRecording", "auditDetail", "loadAudit"].map((n) => [n, slice(n)]);
for (const [name, body] of parts) {
  ok("the page defines " + name, !!body);
}
if (parts.some(([, body]) => !body)) { console.log("FAILED " + failures); process.exit(1); }

/* ---------- one run of the panel against one canned response ---------- */
function run(payload, reject) {
  const nodes = {};
  function node(id) {
    const rows = [];
    return {
      id,
      innerHTML: "",
      style: {},
      rows,
      appendChild(child) { rows.push(child); },
      querySelector() { return nodes[id + ":tbody"] || (nodes[id + ":tbody"] = node(id + ":tbody")); },
    };
  }
  const env = {
    $: (id) => (nodes[id] = nodes[id] || node(id)),
    hideMsg(id) { env.$(id).innerHTML = ""; },
    showMsg(id, cls, text) { env.$(id).innerHTML = '<div class="msg ' + cls + '">' + text + "</div>"; },
    escapeHtml: (s) => String(s == null ? "" : s),
    fmtTime: (ms) => "t" + ms,
    gwBase: () => "",
    apiFetch: () => (reject ? Promise.reject(new Error(reject)) : Promise.resolve(payload)),
    document: { createElement: () => node("tr") },
  };

  const src = parts.map(([, body]) => body).join("\n") + "\n; return loadAudit;";
  const loadAudit = new Function(...Object.keys(env), src)(...Object.values(env));
  loadAudit();
  /* The page's loaders do not return their chain -- none of them do, and asking this one to would
     make it the odd one out for the sake of a harness. So the queue is drained instead: the stub
     settles immediately, and what is being waited for is the .then that writes into the panel.
     A single tick is not enough; that is how a check of this shape passes on a panel that never
     rendered anything. */
  return flush().then(() => ({
    recording: env.$("auditRecording").innerHTML,
    message: env.$("auditMsg").innerHTML,
    table: env.$("auditTable"),
    body: env.$("auditTable").querySelector().rows,
  }));
}

async function flush() {
  for (let i = 0; i < 8; i += 1) {
