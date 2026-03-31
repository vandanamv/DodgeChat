import { INITIAL_ASSISTANT_MESSAGE, STORAGE_KEY } from "../../lib/constants.js";

export function withInitialAssistantMessage(history) {
  const normalized = Array.isArray(history) ? history.slice() : [];
  if (
    normalized.length === 0 ||
    normalized[0]?.role !== "assistant" ||
    normalized[0]?.content !== INITIAL_ASSISTANT_MESSAGE
  ) {
    normalized.unshift({ role: "assistant", content: INITIAL_ASSISTANT_MESSAGE });
  }
  return normalized;
}

export function loadSessionsFromStorage() {
  try {
    const raw = globalThis.localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export function persistSessionsToStorage(sessions) {
  globalThis.localStorage.setItem(STORAGE_KEY, JSON.stringify(sessions.slice(0, 20)));
}

export function buildSessionTitle(history) {
  const firstUser = history.find((item) => item.role === "user");
  if (!firstUser) {
    return "New chat";
  }
  return firstUser.content.length > 42 ? `${firstUser.content.slice(0, 42)}...` : firstUser.content;
}

export function firstUserPrompt(history) {
  const firstUser = history.find((item) => item.role === "user");
  return firstUser ? firstUser.content : "No prompt yet.";
}
