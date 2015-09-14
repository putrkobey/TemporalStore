// Drives the SHIPPED stream client through a goodbye, and reports what the page did about it.
//
// A rotation and a fault are both `event: bye`. The client used to set `planned` for either, and
// `planned` is exactly what makes the next reconnect immediate -- so a gateway that had just said
// it was failing got reconnected into at once, by a page showing nothing wrong.
//
// Usage: node bye_reason_harness.js <page.html> '{"reason":"server_error","incident":"abc"}'
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3] || "{}");

function sliceFn(marker) {
  const start = page.indexOf(marker);
  if (start < 0) throw new Error("not found: " + marker);
  let depth = 0;
  for (let i = page.indexOf("{", start); i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + marker);
}

const ORIGIN = 1_760_000_000_000;
const clockStart = page.indexOf("var liveServerMs = 0;");
const source = page.slice(clockStart, page.indexOf("function liveStream(options)"))
  + "\n" + sliceFn("function liveStream(options)");

let clock = ORIGIN;
const states = [];

/* The stream opens, sends one frame, then says goodbye and ends. Ending it is what makes the
   client decide whether to reconnect at once or back off, which is the behaviour under test. */
const chunks = [
  "retry: 3000\n\n" + "event: status\ndata: " + JSON.stringify({ ts: 1760000000, tick_s: 2 }) + "\n\n",
];

/* A PLANNED goodbye reconnects at once, so a harness that serves a fresh stream every time is an
   infinite loop -- it was, and it exhausted the heap twice. The second connection never answers,
   rather than refusing: refusing drives the client round again. This bounds the run at one
   reconnect and still records that one was attempted. */
let opens = 0;

const scope = {
  Date: { now: () => clock },
  fetch: () => (++opens > 1 ? new Promise(() => {}) : Promise.resolve({
    ok: true,
    body: {
      getReader: () => {
        let step = 0;
        return {
          read: () => {
            step += 1;
