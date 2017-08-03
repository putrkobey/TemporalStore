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
  .map((tag) => ({ tag, id: (tag.match(/\sid="([^"]*)"/) || [])[1] }))
  .filter((d) => d.id);

ok("the page folds something away", foldTags.length > 0);
ok("a link can name it", named.length > 0,
   "no <details> carries an id, so nothing can be linked to");
if (!named.length) { console.log("FAILED " + failures); process.exit(1); }

named.forEach((fold) => {
  ok("#" + fold.id + " starts shut", !/\sopen[\s>]/.test(fold.tag),
     "it is open in the markup, so nothing below is testing anything");
});

/* ---------- a DOM with the one relationship that matters ---------- */
function makeEl(spec) {
  return {
    id: spec.id || null,
    tagName: spec.tagName,
    open: false,
    scrolled: 0,
    parentNode: spec.parentNode || null,
    scrollIntoView() { this.scrolled += 1; },
  };
}

const src = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)]
  .map((m) => m[1]).find((s) => s.includes("function reveal()"));
if (!src) { console.log("FAIL the page carries no reveal helper"); process.exit(1); }

/* `nest` puts the named element INSIDE the fold rather than being it -- the walk has to climb, and
   a helper that only ever looked at the element itself would pass every test that does not. */
