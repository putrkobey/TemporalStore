import crypto from "node:crypto";
import os from "node:os";

function firstStringAt(payload, paths) {
  for (const path of paths) {
    let value = payload;
    for (const part of path) {
      if (!value || typeof value !== "object" || !(part in value)) {
        value = undefined;
        break;
      }
      value = value[part];
    }
    if (typeof value === "string" && value.trim()) {
      return { value: value.trim(), source: path.join(".") };
    }
  }
  return { value: "", source: "" };
}

function shortHash(value) {
  return crypto.createHash("sha256").update(String(value)).digest("hex").slice(0, 16);
}

function firstStringEnv(env, keys) {
  for (const key of keys) {
    const value = env[key];
    if (typeof value === "string" && value.trim()) {
      return { value: value.trim(), source: key };
    }
  }
  return { value: "", source: "" };
