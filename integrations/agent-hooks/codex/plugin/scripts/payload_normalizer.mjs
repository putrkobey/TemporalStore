import { resolveSessionId, resolveWorkspace } from "./session_resolver.mjs";

function textFromPayload(payload) {
  for (const key of [
    "prompt",
    "user_prompt",
    "userPrompt",
    "text",
    "input",
    "message",
    "assistant_message",
    "assistantMessage",
    "last_agent_message",
    "lastAgentMessage",
    "last_assistant_message",
    "lastAssistantMessage",
    "response",
    "output",
    "tool_output",
    "toolOutput",
    "terminal_output",
    "terminalOutput"
  ]) {
    if (typeof payload[key] === "string" && payload[key].trim()) {
      return payload[key].trim();
    }
  }
  for (const path of [
    ["params", "prompt"],
    ["params", "text"],
    ["params", "output"],
    ["params", "tool_output"],
    ["params", "assistant_message"],
    ["turn", "text"],
    ["turn", "assistant_message"],
    ["tool", "output"],
    ["tool", "result"],
    ["metadata", "prompt"],
    ["metadata", "text"],
    ["metadata", "output"],
    ["metadata", "assistant_message"]
  ]) {
    let value = payload;
    for (const part of path) {
      if (!value || typeof value !== "object" || !(part in value)) {
        value = undefined;
        break;
      }
      value = value[part];
    }
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  if (Array.isArray(payload.output)) {
    const parts = [];
    for (const item of payload.output) {
