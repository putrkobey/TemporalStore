const fs = require("fs");
const path = require("path");
const { marked } = require("marked");
const { chromium } = require("playwright");

function decodeHtml(value) {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

async function main() {
  const input = process.argv[2];
  const outputPdf = process.argv[3];
  const outputHtml = process.argv[4] || outputPdf.replace(/\.pdf$/i, ".html");

  if (!input || !outputPdf) {
    console.error("usage: node render_markdown_mermaid_pdf.cjs input.md output.pdf [output.html]");
    process.exit(2);
  }

  const md = fs.readFileSync(input, "utf8");
  let body = marked.parse(md, { mangle: false, headerIds: true });
  body = body.replace(
    /<pre><code class="language-mermaid">([\s\S]*?)<\/code><\/pre>/g,
    (_, code) => `<div class="mermaid">${decodeHtml(code).trim()}</div>`
  );

  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>TemporalStore</title>
  <style>
    @page { size: Letter; margin: 0.55in; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      color: #1f2933;
      line-height: 1.48;
