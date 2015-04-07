import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function pluginRoot() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  return path.resolve(scriptDir, "..");
}

export function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const body = fs.readFileSync(filePath, "utf8");
  for (const rawLine of body.split(/\r?\n/)) {
