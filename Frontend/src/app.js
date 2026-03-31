import { elements, state } from "./core/state.js";
import { setStatus } from "./lib/utils.js";
import { bindGraphUiEvents, hideNodeCard, initGraph, zoomGraph } from "./components/graph.js";
import {
  cancelEditingMessage,
  closeHistoryPanel,
  initChatComposer,
  loadSessions,
  openHistoryPanel,
  renderHistoryPanel,
  sendQuestion,
  startNewSession,
} from "./components/chat.js";

const GRAPH_ZOOM_STEP = 1.18;

function toggleNavMenu() {
  const isHidden = elements.navMenu.classList.contains("hidden");
  elements.navMenu.classList.toggle("hidden", !isHidden);
  elements.navToggleButton.setAttribute("aria-expanded", String(isHidden));
}

function closeNavMenu() {
  elements.navMenu.classList.add("hidden");
  elements.navToggleButton.setAttribute("aria-expanded", "false");
}

function toggleMappingMenu() {
  const isHidden = elements.mappingSubmenu.classList.contains("hidden");
  elements.mappingSubmenu.classList.toggle("hidden", !isHidden);
  updateMappingMenuButton(isHidden);
}

function updateMappingMenuButton(isExpanded) {
  const mappingButton = document.querySelector('[data-nav-action="toggle-mapping"]');
  if (!mappingButton) {
    return;
  }
  mappingButton.setAttribute("aria-expanded", String(isExpanded));
  const caret = mappingButton.querySelector(".nav-menu-caret");
  if (caret) {
    caret.textContent = isExpanded ? "-" : "+";
  }
}

function handleNavAction(action) {
  const navActions = {
    home: () => {
      startNewSession();
      closeHistoryPanel();
    },
    history: openHistoryPanel,
    settings: () => {
      closeHistoryPanel();
      setStatus("Settings will be available here soon.");
    },
    "mapping-order-to-cash": () => {
      closeHistoryPanel();
      setStatus("Order to Cash mapping is active.");
    },
    "toggle-mapping": () => {
      toggleMappingMenu();
      return false;
    },
  };

  const handler = navActions[action];
  if (!handler) {
    closeNavMenu();
    return;
  }
  const shouldCloseNav = handler();
  if (shouldCloseNav !== false) {
    closeNavMenu();
  }
}

async function loadGraph() {
  const response = await fetch("/api/graph");
  const graph = await response.json();
  initGraph(graph);
  loadSessions();
  renderHistoryPanel();
  startNewSession();
  setStatus("Dodge AI is awaiting instructions.");
}

function handlePromptSubmitKeydown(event) {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    sendQuestion();
  }
}

function handleAskAboutSelectedNode() {
  if (!state.selectedNodeId || !state.cy) {
    return;
  }
  const node = state.cy.getElementById(state.selectedNodeId);
  elements.promptInput.value = `Explain the role and connected records for ${node.data("entity")} ${node.data("label")}.`;
  elements.promptInput.focus();
}

function clearGraphHighlights() {
  if (!state.cy) {
    return;
  }
  state.cy.elements().removeClass("dimmed highlighted search-hit search-edge");
  hideNodeCard({ clearHighlights: true });
}

function closeNavMenuOnOutsideClick(event) {
  if (!elements.navMenu.contains(event.target) && !elements.navToggleButton.contains(event.target)) {
    closeNavMenu();
  }
}

function bindUiEvents() {
  elements.navToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleNavMenu();
  });

  elements.navMenu.addEventListener("click", (event) => {
    const actionTarget = event.target.closest("[data-nav-action]");
    if (!actionTarget) {
      return;
    }
    handleNavAction(actionTarget.dataset.navAction);
  });

  elements.sendButton.addEventListener("click", sendQuestion);
  elements.cancelEditButton.addEventListener("click", cancelEditingMessage);
  elements.updateEditButton.addEventListener("click", sendQuestion);
  elements.promptInput.addEventListener("keydown", handlePromptSubmitKeydown);

  elements.closeHistoryButton.addEventListener("click", () => {
    startNewSession();
    closeHistoryPanel();
  });
  elements.openHistoryButton.addEventListener("click", openHistoryPanel);
  elements.newChatButton.addEventListener("click", () => {
    startNewSession();
    closeHistoryPanel();
  });
  elements.askAboutNode.addEventListener("click", handleAskAboutSelectedNode);

  elements.fitButton.addEventListener("click", () => state.cy && state.cy.fit(undefined, 28));
  elements.zoomInButton.addEventListener("click", () => zoomGraph(GRAPH_ZOOM_STEP));
  elements.zoomOutButton.addEventListener("click", () => zoomGraph(1 / GRAPH_ZOOM_STEP));
  elements.resetButton.addEventListener("click", clearGraphHighlights);

  globalThis.addEventListener("click", closeNavMenuOnOutsideClick);

  bindGraphUiEvents();
  initChatComposer();
}

bindUiEvents();
loadGraph().catch((error) => setStatus(String(error.message || error), true));
