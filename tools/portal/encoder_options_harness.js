/* Run the setup page's modelOptions and see which encoders it OFFERS.
 *
 * The measured catalogue is five self-hosted models, and the numbers beside them come from encoding
 * a real corpus on the machine that runs them. A deployment on Voyage cannot serve any of them, and
 * was offered all five and none of the ones it can.
 *
 * The fix is a split, not a filter on one list: `applicable` narrows what is OFFERED without
 * narrowing what is SHOWN, so the comparison table still answers "what would self-hosting score".
 * That distinction only exists in the dropdown builder, so it is pulled out of the BUILT page and
 * executed here.
 *
 * Usage: node encoder_options_harness.js <setup_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

const start = page.indexOf("function modelOptions(target) {");
if (start < 0) { console.log("FAIL modelOptions is not on the page"); process.exit(1); }
const end = page.indexOf("\n  }", start);
if (end < 0) { console.log("FAIL no end for modelOptions"); process.exit(1); }
const source = page.slice(start, end + 4);

function options(routeBody) {
  const modelData = { embedding: routeBody };
  const scope = new Proxy({}, {
    has: () => true,
    get: (_t, name) => {
      if (name === Symbol.unscopables) { return undefined; }
      if (name === "modelData") { return modelData; }
      if (name in globalThis) { return globalThis[name]; }
      return () => "";
    }
  });
  const NL = String.fromCharCode(10);
  const body = "return function build() {" + NL + source + NL +
    "return modelOptions('embedding');" + NL + "};";
  return new Function("scope", "with (scope) { " + body + " }")(scope)();
}

const MEASURED = [
  { model: "intfloat/multilingual-e5-large", dim: 512, hit_at_1: 76.2, texts_per_s: 1.7,
    note: "best measured", same_width_as: [] },
  { model: "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", dim: 384, hit_at_1: 59.4,
    texts_per_s: 31.7, note: "fastest", same_width_as: [] }
];

