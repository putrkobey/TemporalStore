/* Run the Setup page and check it does not read the encoding backlog twice.
 *
 * `onFrame` already calls renderEncoding with the backlog the stream carries. A timer also fetches
 * /v1/admin/embeddings, and that endpoint walks the record log on the backend. While the stream is
 * live, the timer is a second read of one answer.
 *
 * Source cannot answer this. Whether the timer fetches depends on a closure variable set by a
 * callback the stream invokes on connecting -- so the page is run twice: once with a stream that
 * connects, once with one that fails. The second run is what stops the first from being vacuous:
 * a page that never polls at all would satisfy "does not poll while live" and be broken.
 *
 * Usage: node encoding_poll_harness.js <setup_portal.html>
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

const FRAME = 'event: status\ndata: {"ts":1,"traffic":{"routes":{}},"imports":{},"warnings":0,' +
              '"embedding":{"total":10,"encoded":10,"pending":0,"encoder":{}}}\n\n';

function run(streamWorks) {
  const fetched = [];
  const intervals = [];
  const els = new Map();

  function makeEl(id) {
    return {
      id: id || "", hidden: true, innerHTML: "", textContent: "",
      /* The timer returns early without a key. A harness that forgot would see no fetch and call
         the gate proven. */
      value: id === "key" ? "k-admin" : "",
      checked: true, className: "", style: {}, dataset: {}, files: [], options: [], children: [],
      addEventListener() {}, removeEventListener() {}, appendChild() {}, removeChild() {},
      setAttribute() {}, getAttribute() { return null; }, closest() { return null; },
      querySelector() { return null; }, querySelectorAll() { return []; },
      scrollIntoView() {}, focus() {}, click() {},
      classList: { add() {}, remove() {}, toggle() {} }
    };
  }
  function el(id) {
    if (!els.has(id)) { els.set(id, makeEl(id)); }
    return els.get(id);
  }

  function streamingResponse() {
    let sent = false;
    return Promise.resolve({
      ok: true, status: 200,
      body: {
        getReader() {
          return {
            read() {
              if (sent) { return new Promise(() => {}); }   // stays open, like a real stream
              sent = true;
              return Promise.resolve({ done: false, value: Buffer.from(FRAME, "utf8") });
            }
          };
        }
      },
      json: () => Promise.resolve({}), text: () => Promise.resolve("")
    });
  }

  function json(body) {
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body),
                             text: () => Promise.resolve(JSON.stringify(body)) });
  }

  const win = {
    document: {
      getElementById: (id) => el(id),
      querySelector: () => makeEl(), querySelectorAll: () => [],
      createElement: () => makeEl(), addEventListener() {},
