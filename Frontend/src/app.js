import { elements, state } from "./core/state.js";
import { setStatus } from "./lib/utils.js";
import { bindGraphUiEvents, hideNodeCard, initGraph, zoomGraph } from "./components/graph.js";
import {
  closeHistoryPanel,
  loadSessions,
  openHistoryPanel,
  renderHistoryPanel,
  sendQuestion,
  startNewSession,
} from "./components/chat.js";

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
  const mappingButton = document.querySelector('[data-nav-action="toggle-mapping"]');
  if (mappingButton) {
    mappingButton.setAttribute("aria-expanded", String(isHidden));
    const caret = mappingButton.querySelector(".nav-menu-caret");
    if (caret) {
      caret.textContent = isHidden ? "-" : "+";
    }
  }
}

function handleNavAction(action) {
  if (action === "home") {
    startNewSession();
    closeHistoryPanel();
  } else if (action === "history") {
    openHistoryPanel();
  } else if (action === "settings") {
    closeHistoryPanel();
    setStatus("Settings will be available here soon.");
  } else if (action === "mapping-order-to-cash") {
    closeHistoryPanel();
    setStatus("Order to Cash mapping is active.");
  } else if (action === "toggle-mapping") {
    toggleMappingMenu();
    return;
  }
  closeNavMenu();
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
  elements.promptInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendQuestion();
    }
  });

  elements.closeHistoryButton.addEventListener("click", closeHistoryPanel);
  elements.askAboutNode.addEventListener("click", () => {
    if (!state.selectedNodeId || !state.cy) {
      return;
    }
    const node = state.cy.getElementById(state.selectedNodeId);
    elements.promptInput.value = `Explain the role and connected records for ${node.data("entity")} ${node.data("label")}.`;
    elements.promptInput.focus();
  });

  elements.fitButton.addEventListener("click", () => state.cy && state.cy.fit(undefined, 28));
  elements.zoomInButton.addEventListener("click", () => zoomGraph(1.18));
  elements.zoomOutButton.addEventListener("click", () => zoomGraph(1 / 1.18));
  elements.resetButton.addEventListener("click", () => {
    if (!state.cy) {
      return;
    }
    state.cy.elements().removeClass("dimmed highlighted search-hit search-edge");
    hideNodeCard({ clearHighlights: true });
  });

  globalThis.addEventListener("click", (event) => {
    if (!elements.navMenu.contains(event.target) && !elements.navToggleButton.contains(event.target)) {
      closeNavMenu();
    }
  });

  bindGraphUiEvents();
}

bindUiEvents();
loadGraph().catch((error) => setStatus(String(error.message || error), true));
