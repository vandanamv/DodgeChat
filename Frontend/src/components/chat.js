import { elements, state } from "../core/state.js";
import { INITIAL_ASSISTANT_MESSAGE, STORAGE_KEY } from "../lib/constants.js";
import { setStatus } from "../lib/utils.js";
import { applyRelatedHighlights, focusGraphOnNodeIds, hideNodeCard } from "./graph.js";

export function addMessage(role, text) {
  const wrapper = document.createElement("div");
  wrapper.className = `message ${role}`;
  const head = document.createElement("div");
  head.className = "message-head";
  const meta = document.createElement("div");
  meta.className = "message-meta";
  const name = document.createElement("div");
  name.className = "message-name";
  name.textContent = role === "assistant" ? "Dodge AI" : "You";
  meta.appendChild(name);

  if (role === "assistant") {
    const roleLabel = document.createElement("div");
    roleLabel.className = "message-role";
    roleLabel.textContent = "Graph Agent";
    meta.appendChild(roleLabel);
  }

  const avatar = document.createElement("div");
  avatar.className = `message-avatar ${role}`;
  avatar.textContent = role === "assistant" ? "D" : "U";

  if (role === "assistant") {
    head.appendChild(avatar);
    head.appendChild(meta);
  } else {
    head.appendChild(meta);
    head.appendChild(avatar);
  }

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.textContent = text;
  wrapper.appendChild(head);
  wrapper.appendChild(bubble);

  elements.messages.appendChild(wrapper);
  elements.messages.scrollTop = elements.messages.scrollHeight;
}

export function renderMessages() {
  elements.messages.innerHTML = "";
  state.history.forEach((item) => addMessage(item.role, item.content));
}

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

export function loadSessions() {
  try {
    const raw = globalThis.localStorage.getItem(STORAGE_KEY);
    state.sessions = raw ? JSON.parse(raw) : [];
  } catch {
    state.sessions = [];
  }
}

function persistSessions() {
  globalThis.localStorage.setItem(STORAGE_KEY, JSON.stringify(state.sessions.slice(0, 20)));
}

function sessionTitleFromHistory(history) {
  const firstUser = history.find((item) => item.role === "user");
  if (!firstUser) {
    return "New chat";
  }
  return firstUser.content.length > 42 ? `${firstUser.content.slice(0, 42)}...` : firstUser.content;
}

export function renderHistoryPanel() {
  elements.historyList.innerHTML = "";
  if (state.sessions.length === 0) {
    const empty = document.createElement("div");
    empty.className = "history-empty";
    empty.textContent = "No saved chats yet.";
    elements.historyList.appendChild(empty);
    return;
  }
  state.sessions.forEach((session) => {
    const button = document.createElement("button");
    button.className = "history-item";
    button.type = "button";
    button.dataset.sessionId = session.id;

    const title = document.createElement("div");
    title.className = "history-item-title";
    title.textContent = session.title || "New chat";

    const meta = document.createElement("div");
    meta.className = "history-item-meta";
    meta.textContent = `${new Date(session.updatedAt).toLocaleString()} - ${session.history.length} messages`;

    const preview = document.createElement("div");
    preview.className = "history-item-preview";
    const lastAssistant = [...session.history].reverse().find((item) => item.role === "assistant");
    preview.textContent = lastAssistant ? lastAssistant.content : "No assistant reply yet.";

    button.appendChild(title);
    button.appendChild(meta);
    button.appendChild(preview);
    button.addEventListener("click", () => openSession(session.id));
    elements.historyList.appendChild(button);
  });
}

function syncActiveSession() {
  if (!state.activeSessionId) {
    state.activeSessionId = `session-${Date.now()}`;
  }
  const session = {
    id: state.activeSessionId,
    title: sessionTitleFromHistory(state.history),
    updatedAt: new Date().toISOString(),
    history: withInitialAssistantMessage(state.history),
  };
  state.sessions = [session, ...state.sessions.filter((item) => item.id !== session.id)].slice(0, 20);
  persistSessions();
  renderHistoryPanel();
}

export function openHistoryPanel() {
  renderHistoryPanel();
  elements.historyPanel.classList.remove("hidden");
}

export function closeHistoryPanel() {
  elements.historyPanel.classList.add("hidden");
}

function openSession(sessionId) {
  const session = state.sessions.find((item) => item.id === sessionId);
  if (!session) {
    return;
  }
  state.activeSessionId = session.id;
  state.history = withInitialAssistantMessage(session.history);
  renderMessages();
  closeHistoryPanel();
}

export function startNewSession() {
  state.activeSessionId = `session-${Date.now()}`;
  state.history = withInitialAssistantMessage([]);
  state.selectedNodeId = null;
  renderMessages();
  hideNodeCard();
  setStatus("Dodge AI is awaiting instructions.");
}

export async function sendQuestion() {
  const question = elements.promptInput.value.trim();
  if (!question) {
    return;
  }
  elements.promptInput.value = "";
  addMessage("user", question);
  state.history.push({ role: "user", content: question });
  syncActiveSession();
  setStatus("Analyzing your question...");
  try {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, history: state.history, focusNodeId: state.selectedNodeId }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Request failed.");
    }
    addMessage("assistant", payload.answer);
    state.history.push({ role: "assistant", content: payload.answer });
    syncActiveSession();
    applyRelatedHighlights(payload.relatedNodeIds || []);
    focusGraphOnNodeIds(payload.relatedNodeIds || []);
    setStatus("Dodge AI is awaiting instructions.");
  } catch (error) {
    addMessage("assistant", String(error.message || error));
    setStatus(String(error.message || error), true);
  }
}
