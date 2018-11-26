/* Does the ingestion panel notice the configuration changing under it?
 *
 * That panel renders what an import will actually use -- embedding provider and model, the encoder
 * endpoint, whether a failed encoder call errors or falls back, the extraction provider and key --
 * and all of it is set on a different page. It read that once, on load, and went on describing the
 * old deployment for as long as the tab stayed open, under a header saying "checked 14:32:01":
 * true about when it looked, false about what it is showing.
 *
 * Whether a watcher fires is behaviour, and the ways it goes wrong are all silent: registering
 * against a function the nav has not defined yet, firing on the first frame, firing on every frame,
 * firing for a hidden tab. So the page's own registration is executed and the callback it hands
 * over is driven with frames.
 *
 * Usage: node config_watch_harness.js <ingestion_portal.html>
 */
"use strict";
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");

let failures = 0;
