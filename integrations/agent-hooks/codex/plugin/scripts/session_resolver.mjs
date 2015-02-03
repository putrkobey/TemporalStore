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
}

export function resolveSessionId(payload = {}, env = process.env) {
  const direct = firstStringAt(payload, [
    ["session_id"],
    ["sessionId"],
    ["codex_session_id"],
    ["conversation_id"],
    ["conversationId"],
    ["thread_id"],
    ["threadId"],
    ["transcript_id"],
    ["transcriptId"],
    ["run", "session_id"],
    ["run", "thread_id"],
    ["params", "session_id"],
    ["params", "thread_id"],
    ["turn", "session_id"],
    ["turn", "thread_id"],
    ["metadata", "session_id"],
    ["metadata", "thread_id"]
  ]);
  if (direct.value) {
    return {
      sessionId: `${env.TEMPORALSTORE_AGENT_NAME || "codex"}:${direct.value}`,
      conversationId: direct.value,
      source: `payload.${direct.source}`
    };
  }

  const pathValue = firstStringAt(payload, [
    ["transcript_path"],
