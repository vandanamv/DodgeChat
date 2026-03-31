import { elements, state } from "../core/state.js";
import { setStatus } from "../lib/utils.js";
import { applyRelatedHighlights, focusGraphOnNodeIds, hideNodeCard } from "./graph.js";
import { ANALYZING_STATUS, EDITING_STATUS, IDLE_STATUS } from "./chat_features/composer_constants.js";
import {
  createHistoryEmptyElement,
  createHistoryItemElement,
} from "./chat_features/history_panel.js";
import { createMessageElement } from "./chat_features/message_list.js";
import {
  buildSessionTitle,
  firstUserPrompt,
  loadSessionsFromStorage,
  persistSessionsToStorage,
  withInitialAssistantMessage,
} from "./chat_features/conversation_store.js";

function setHistoryMode(isOpen) {
  elements.chatSection.classList.toggle("history-mode", isOpen);
}

function scrollMessagesToBottom() {
  elements.messages.scrollTop = elements.messages.scrollHeight;
}

function clearPromptInput() {
  elements.promptInput.value = "";
}

function focusPromptInputAtEnd(value) {
  elements.promptInput.value = value;
  elements.promptInput.focus();
  elements.promptInput.setSelectionRange(value.length, value.length);
}

function updateComposerUi() {
  const isEditing = Number.isInteger(state.editingMessageIndex);
  elements.composerMode.classList.toggle("hidden", !isEditing);
  elements.promptInput.classList.toggle("is-editing", isEditing);
  elements.composerMode.textContent = isEditing ? EDITING_STATUS : "";
  elements.cancelEditButton.classList.toggle("hidden", !isEditing);
  elements.updateEditButton.classList.toggle("hidden", !isEditing);
  elements.sendButton.classList.toggle("hidden", isEditing);
  elements.sendButton.disabled = state.isSending;
  elements.cancelEditButton.disabled = state.isSending;
  elements.updateEditButton.disabled = state.isSending;
}

function startEditingMessage(index) {
  const item = state.history[index];
  if (!item || item.role !== "user") {
    return;
  }
  state.editingMessageIndex = index;
  focusPromptInputAtEnd(item.content);
  updateComposerUi();
  setStatus("Edit the prompt and apply your update.");
}

function resetToEmptyConversation(statusMessage = IDLE_STATUS) {
  state.activeSessionId = null;
  state.history = withInitialAssistantMessage([]);
  state.editingMessageIndex = null;
  renderMessages();
  updateComposerUi();
  hideNodeCard();
  setStatus(statusMessage);
}

export function cancelEditingMessage() {
  state.editingMessageIndex = null;
  clearPromptInput();
  updateComposerUi();
  setStatus(IDLE_STATUS);
}

export function addMessage(role, text, index) {
  elements.messages.appendChild(createMessageElement(role, text, index, startEditingMessage));
}

export function renderMessages() {
  elements.messages.innerHTML = "";
  state.history.forEach((item, index) => addMessage(item.role, item.content, index));
  scrollMessagesToBottom();
}

export function loadSessions() {
  state.sessions = loadSessionsFromStorage();
}

function deleteSession(sessionId) {
  state.sessions = state.sessions.filter((item) => item.id !== sessionId);
  if (state.activeSessionId === sessionId) {
    resetToEmptyConversation("Chat deleted. Dodge AI is awaiting instructions.");
  }
  persistSessionsToStorage(state.sessions);
  renderHistoryPanel();
}

export function renderHistoryPanel() {
  elements.historyList.innerHTML = "";
  if (state.sessions.length === 0) {
    elements.historyList.appendChild(createHistoryEmptyElement());
    return;
  }
  state.sessions.forEach((session) => {
    elements.historyList.appendChild(
      createHistoryItemElement(session, firstUserPrompt(session.history), openSession, deleteSession)
    );
  });
}

function syncActiveSession() {
  if (!state.activeSessionId) {
    state.activeSessionId = `session-${Date.now()}`;
  }
  const session = {
    id: state.activeSessionId,
    title: buildSessionTitle(state.history),
    updatedAt: new Date().toISOString(),
    history: withInitialAssistantMessage(state.history),
  };
  state.sessions = [session, ...state.sessions.filter((item) => item.id !== session.id)].slice(0, 20);
  persistSessionsToStorage(state.sessions);
  renderHistoryPanel();
}

export function openHistoryPanel() {
  renderHistoryPanel();
  setHistoryMode(true);
  elements.historyPanel.classList.remove("hidden");
}

export function closeHistoryPanel() {
  setHistoryMode(false);
  elements.historyPanel.classList.add("hidden");
}

function openSession(sessionId) {
  const session = state.sessions.find((item) => item.id === sessionId);
  if (!session) {
    return;
  }
  state.activeSessionId = session.id;
  state.history = withInitialAssistantMessage(session.history);
  state.editingMessageIndex = null;
  renderMessages();
  updateComposerUi();
  closeHistoryPanel();
}

export function startNewSession() {
  state.activeSessionId = `session-${Date.now()}`;
  state.selectedNodeId = null;
  state.history = withInitialAssistantMessage([]);
  state.editingMessageIndex = null;
  renderMessages();
  updateComposerUi();
  hideNodeCard();
  setStatus(IDLE_STATUS);
}

export async function sendQuestion() {
  const question = elements.promptInput.value.trim();
  if (!question || state.isSending) {
    return;
  }
  const editingIndex = state.editingMessageIndex;
  state.editingMessageIndex = null;
  state.isSending = true;
  updateComposerUi();
  clearPromptInput();
  if (Number.isInteger(editingIndex)) {
    state.history = state.history.slice(0, editingIndex);
  }
  state.history.push({ role: "user", content: question });
  renderMessages();
  syncActiveSession();
  setStatus(ANALYZING_STATUS);
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
    state.history.push({ role: "assistant", content: payload.answer });
    renderMessages();
    syncActiveSession();
    applyRelatedHighlights(payload.relatedNodeIds || []);
    focusGraphOnNodeIds(payload.relatedNodeIds || []);
    setStatus(IDLE_STATUS);
  } catch (error) {
    state.history.push({ role: "assistant", content: String(error.message || error) });
    renderMessages();
    syncActiveSession();
    setStatus(String(error.message || error), true);
  } finally {
    state.isSending = false;
    updateComposerUi();
  }
}

export function initChatComposer() {
  updateComposerUi();
}
