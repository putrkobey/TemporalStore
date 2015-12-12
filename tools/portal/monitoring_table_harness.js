/* Run the Setup page's scripts against a real /v1/admin/config payload and report what the
 * monitoring section actually rendered.
 *
 * Grepping the built page cannot tell these apart: a table that renders five rows, and one whose
 * builder is never reached because the payload it reads is shaped differently than the server
 * sends. Both contain the same source text. The specific thing worth proving here is that the
 * engine row says /metrics -- the whole point of the column is that importing that dashboard
 * against the gateway yields blank panels, and a blank panel reads as a quiet cluster.
 *
 * The stub keeps element identity per id, unlike the watcher harness next to it: this test reads
 * back what the page wrote into #monitoring and #scrape, so a fresh element per lookup would
 * discard exactly the thing under test.
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const payload = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);

const fetched = [];
const byId = new Map();

function makeEl(id) {
  const listeners = {};
  const attrs = {};
  const el = {
    id: id || "",
    hidden: true, innerHTML: "", textContent: "", value: "", checked: false, className: "",
