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
