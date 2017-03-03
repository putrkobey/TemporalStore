// Drives the SHIPPED live-stream parser with a real SSE block, then asks the SHIPPED agoText and
// clockNoteHtml what they say. Setting the clock variables directly would test my arithmetic, not
// the code that reads `ts` off a frame -- which is the part that did not exist.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
const input = JSON.parse(process.argv[3]);

function slice(startMarker) {
  const start = page.indexOf(startMarker);
  if (start < 0) throw new Error("not found: " + startMarker);
  let depth = 0, i = page.indexOf("{", start);
  for (; i < page.length; i++) {
    if (page[i] === "{") depth++;
    else if (page[i] === "}") { depth--; if (depth === 0) return page.slice(start, i + 1); }
  }
  throw new Error("unclosed: " + startMarker);
}

// The clock block is plain statements, not a function: take it up to `function liveStream`.
const clockStart = page.indexOf("var liveServerMs = 0;");
const clockBlock = page.slice(clockStart, page.indexOf("function liveStream(options)"));

const parts = [
