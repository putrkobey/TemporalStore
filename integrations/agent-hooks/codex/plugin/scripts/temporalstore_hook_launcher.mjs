#!/usr/bin/env node
import { loadPayloadFromStdin, callLocalPythonHook, callRemoteHook } from "./temporalstore_client.mjs";
import { normalizePayload } from "./payload_normalizer.mjs";
import { loadPluginEnv } from "./config_loader.mjs";

function usage() {
  return `Usage: temporalstore_hook_launcher.mjs <event> [--dry-run] [--print-config]\n`;
}

function parseArgs(argv) {
  const args = {
    event: "UserPromptSubmit",
    dryRun: false,
