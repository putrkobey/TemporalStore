import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function pluginRoot() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
