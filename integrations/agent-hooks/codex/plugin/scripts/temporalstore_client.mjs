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

