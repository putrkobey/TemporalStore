/* Follow a link that names something folded away.
 *
 * The key portal answers "where does the first admin key come from" inside a <details>, shut by
 * default because it is a question somebody has once. Three other pages link straight at it. A link
 * to a closed <details> arrives with the answer still folded up -- the reader is on the right page,
 * looking at a summary line, no worse informed than before -- and browsers disagree about whether
 * they expand it, so leaving it to them means it works for some customers and not others.
 *
 * The page's own helper is executed here rather than read: whether a <details> ends up open is
 * behaviour, and a page whose reveal helper is wired to nothing looks in source exactly like one
 * whose works.
 *
 * Usage: node folded_answer_harness.js <page.html>
 */
"use strict";
const fs = require("fs");

const path = process.argv[2];
const page = fs.readFileSync(path, "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---------- what the page actually declares ---------- */
const markup = page.replace(/<script[\s\S]*?<\/script>/g, (m) => " ".repeat(m.length));
const foldTags = [...markup.matchAll(/<details[^>]*>/g)].map((m) => m[0]);
const named = foldTags
