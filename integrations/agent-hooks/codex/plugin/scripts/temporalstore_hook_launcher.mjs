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
    printConfig: false
  };
  for (const arg of argv) {
    if (arg === "--dry-run") args.dryRun = true;
    else if (arg === "--print-config") args.printConfig = true;
    else if (arg === "-h" || arg === "--help") {
      process.stdout.write(usage());
      process.exit(0);
    } else if (!arg.startsWith("--")) {
      args.event = arg;
    }
  }
  return args;
}
