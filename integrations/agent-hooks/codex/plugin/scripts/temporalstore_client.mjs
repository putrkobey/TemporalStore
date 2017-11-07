import http from "node:http";
import https from "node:https";
import { spawnSync } from "node:child_process";

function readJsonStdin() {
  return new Promise((resolve, reject) => {
    let input = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      input += chunk;
    });
    process.stdin.on("end", () => {
      if (!input.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(input));
      } catch (error) {
        reject(new Error(`failed to parse hook stdin as JSON: ${error.message}`));
      }
    });
  });
}

export async function loadPayloadFromStdin() {
  return readJsonStdin();
}

function hookArgs(event, env) {
  return [
    "tools/matrixark_codex_hook.py",
    "--event",
    event,
    "--backend",
    env.TEMPORALSTORE_HOOK_BACKEND || "temporalstore-direct",
    "--metaserver",
    env.TEMPORALSTORE_METASERVER || "127.0.0.1:18000",
    "--namespace",
    env.TEMPORALSTORE_NAMESPACE || "deploy_ns",
    "--table",
    env.TEMPORALSTORE_TABLE || "deploy_table",
    "--storage-prefix",
    env.MATRIXARK_STORAGE_PREFIX || "matrixark:agent-hook",
    "--account-id",
    env.MATRIXARK_ACCOUNT_ID || "acct_local",
    "--tenant-id",
    env.MATRIXARK_TENANT_ID || "tenant_codex",
    "--user-id",
    env.MATRIXARK_USER_ID || "local_user",
    "--team",
    env.MATRIXARK_TEAM || "agent",
    "--project",
    env.TEMPORALSTORE_AGENT_PROJECT || "TemporalStore",
    "--max-context-tokens",
    env.MATRIXARK_MAX_CONTEXT_TOKENS || "10000",
    "--request-timeout-ms",
    env.MATRIXARK_REQUEST_TIMEOUT_MS || "90000",
