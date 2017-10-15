/* Follow a link that names a place inside a tab, the way the status strip does.
 *
 * The live strip on every portal page links to "/v1/admin/setup#encoding" and
 * "/v1/admin/setup#traffic". Both name a div inside a tab pane, and every pane but the first loads
 * hidden -- so the browser's one attempt to scroll to the fragment finds nothing on screen, the
 * default tab comes up, and the badge that was reporting a problem has quietly delivered the reader
 * somewhere else. A link cannot report that it failed; it went to the page it named.
 *
 * Whether that now works is a question about NESTING -- which pane contains the target -- and a
 * flat stub of a DOM cannot answer it. It would happily agree with any answer, including the wrong
 * one, which is how three panes on the key portal shipped spliced into the wrong parents with every
 * harness green. So containment here is read out of the page's own markup by matching <section>
 * against </section>, and the extents that come out are checked for sanity before anything is built
 * on them.
 *
 * Usage: node deep_link_harness.js <page.html> [#fragment ...]
 */
"use strict";
const fs = require("fs");

const path = process.argv[2];
const wanted = process.argv.slice(3).map((f) => (f.charAt(0) === "#" ? f : "#" + f));
const page = fs.readFileSync(path, "utf8");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
  else { console.log("FAIL " + what + (detail ? "\n     " + detail : "")); failures += 1; }
}

/* ---------- containment, read from the markup ---------- */
/* Script blocks build markup out of strings -- "<section>" appears in them with no closing tag in
   sight -- and an unbalanced tag inside one would run the depth count off the end of the document.
   Blanked rather than removed so every index below still points into the real file. */
const markup = page.replace(/<script[\s\S]*?<\/script>/g, (m) => " ".repeat(m.length));

function extentFrom(start) {
  const scan = /<section\b|<\/section>/g;
  scan.lastIndex = start;
  let depth = 0;
  let match;
  while ((match = scan.exec(markup)) !== null) {
    depth += match[0] === "</section>" ? -1 : 1;
    if (depth === 0) { return match.index + match[0].length; }
  }
  return -1;
}

const panes = [];
const paneOpen = /<section[^>]*class="pane"[^>]*>/g;
let open;
while ((open = paneOpen.exec(markup)) !== null) {
  const id = (open[0].match(/\sid="([^"]*)"/) || [])[1] || null;
  panes.push({ id, start: open.index, end: extentFrom(open.index),
               hidden: /\shidden[\s>]/.test(open[0]) });
}

if (!panes.length) { console.log("FAIL the page declares no panes"); process.exit(1); }

/* ---------- the extents have to be worth trusting ---------- */
/* A scan that silently matched nothing, or ran to the end of the file, would make every
   containment answer below come out "no" -- and every assertion that a link does NOT go somewhere
   would pass. So the extents are checked before they are used. */
ok("every pane's extent was found", panes.every((p) => p.end > p.start),
   panes.map((p) => p.id + ":" + p.start + ".." + p.end).join(" "));
ok("every pane is named", panes.every((p) => p.id));
ok("the panes do not overlap and are in order",
   panes.every((p, i) => i === 0 || panes[i - 1].end <= p.start),
   panes.map((p) => p.id + ":" + p.start + ".." + p.end).join(" "));
ok("no pane's extent swallows the rest of the document",
   panes[panes.length - 1].end <= markup.length);

const ids = new Map();
const idAttr = /\sid="([^"]+)"/g;
let found;
while ((found = idAttr.exec(markup)) !== null) {
  if (!ids.has(found[1])) { ids.set(found[1], found.index); }
}

function paneHolding(id) {
  if (!ids.has(id)) { return null; }
  const at = ids.get(id);
  return panes.find((p) => at > p.start && at < p.end) || null;
}

const inside = [...ids.keys()].filter((id) => {
  const holder = paneHolding(id);
  return holder && holder.id !== id;
});
ok("some element is nested inside a pane", inside.length > 0,
   "nothing is contained by anything; the containment test would be vacuous");

/* ---------- a DOM that knows what contains what ---------- */
function makeEl(spec) {
  return Object.assign({
    id: spec.id,
    hidden: !!spec.hidden,
    tabIndex: 0,
