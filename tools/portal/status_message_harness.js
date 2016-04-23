/* One explanation per gateway status, and the page-specific words that survive.
 *
 * Three pages carried a status map of their own and each carried a different subset. Ingestion --
 * the page that uploads files -- was the one with no sentence for 413, so an upload refused for
 * size read as "The gateway answered 413." on the panel where the cause is obvious to everyone
 * except the reader. Four pages had no map at all.
 *
 * A map is a data structure and looks fine in source whatever it omits, so the page's own helper
 * is executed here and asked about each status.
 *
 * Usage: node status_message_harness.js <page.html> [...]
 */
"use strict";
const fs = require("fs");
const path = require("path");

let failures = 0;
function ok(what, condition, detail) {
  if (condition) { console.log("ok   " + what); }
