/* Run the shipped strip script against real frames and print what it renders.
 *
 * The other tests assert that the source SAYS the right things. This one runs it: a strip that
 * contains the word "unknown" and never reaches that branch reads the same in a grep and is a
 * different product.
 *
 * A DOM stub rather than jsdom -- the strip touches five elements and four properties, and a
 * dependency to test twenty lines is a dependency to keep working.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const start = page.indexOf("/* The shared live strip.");
if (start < 0) { throw new Error("the strip script is not on this page"); }
const scriptStart = page.lastIndexOf("<script>", start) + "<script>".length;
const scriptEnd = page.indexOf("</script>", scriptStart);
const source = page.slice(scriptStart, scriptEnd);

function makeEl(id) {
  return { id, hidden: true, innerHTML: "", className: "", _listeners: {},
           addEventListener() {} };
}
const els = {};
["liveStrip", "liveEnc", "liveImp", "liveReq", "liveWarn", "liveDot"].forEach((id) => {
  els[id] = makeEl(id);
});

const win = {
  document: {
    getElementById: (id) => els[id] || null,
    addEventListener() {},
    hidden: false
  },
  addEventListener() {},
  setTimeout: () => 0,
  sessionStorage: { getItem: () => "" },
  /* Claimed, so the script wires its hooks and then stands down instead of trying to connect.
     Exactly the path a page with its own stream takes. */
  __matrixarkLive: "page",
  Number, JSON, String, Math, Date, TextDecoder: function () {}, AbortController: function () {},
  fetch: () => Promise.reject(new Error("no network in this harness"))
};
