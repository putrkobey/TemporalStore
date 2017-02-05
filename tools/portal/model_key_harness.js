/* Run the setup page's model picker and see which SETTING a pick is written into.
 *
 * The picker used to write every pick into `extraction.model`. The Anthropic extraction path reads
 * MATRIXARK_ANTHROPIC_MODEL and ignores that field entirely, so on such a deployment picking a
 * model changed nothing: the form filled in, the page said it was set, and the model in use stayed
 * on its default. The route now names the field, and the page has to follow it.
 *
 * Both the resolver and the click handler are pulled OUT OF THE BUILT PAGE and executed -- this is
 * the copy a browser runs, and the mistake this replaced (a variable left dangling after the
 * rewrite) was a ReferenceError that no amount of reading the source would have shown.
 *
 * Usage: node model_key_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

function slice(from, to) {
  const start = page.indexOf(from);
  if (start < 0) { console.log("FAIL not on the page: " + from); process.exit(1); }
  const end = page.indexOf(to, start);
  if (end < 0) { console.log("FAIL no end for: " + from); process.exit(1); }
  return page.slice(start, end + to.length);
}

const resolverSource = slice("function modelKey(target) {", "\n  }");
const handlerSource = slice("var button = ev.target.closest",
                            'say($("saveMsg"), entry.title + " set to " + value + " — not saved yet. Save configuration " +\n      "below to apply it.", "info");');

/* One case: what the route said, what the person picked, and what the form holds. */
function run(routeBody, picked, fieldKeys) {
  const modelData = { extraction: routeBody, embedding: {} };
  const MODEL_TARGETS = [
    { target: "extraction", key: "extraction.model", title: "Extraction model", note: "" },
    { target: "embedding", key: "embedding.model", title: "Embedding model", note: "" }
  ];
  const edits = {};
  const pendingPick = {};
  const queried = [];
  const messages = [];
  const fields = {};
  fieldKeys.forEach(function (k) {
    fields[k] = { value: "", closest: () => null, scrollIntoView: () => {} };
  });

  const groups = {
    querySelector(selector) {
      const key = /\[data-key="([^"]+)"\]/.exec(selector);
      queried.push(key ? key[1] : selector);
      return key && Object.prototype.hasOwnProperty.call(fields, key[1]) ? fields[key[1]] : null;
    }
  };

  const scope = new Proxy({}, {
    has: () => true,
    get: (_t, name) => {
      switch (name) {
        case Symbol.unscopables: return undefined;
        case "modelData": return modelData;
        case "MODEL_TARGETS": return MODEL_TARGETS;
        case "edits": return edits;
        case "pendingPick": return pendingPick;
        case "pickedModel": return () => picked;
        case "$": return (id) => (id === "groups" ? groups : { id });
