import { elements, state } from "../../core/state.js";
import { clamp } from "../../lib/utils.js";
import { applyProcessFlowHighlight, isHoverAllowed } from "./graph_highlighting.js";

function updateFocusBadge() {
  if (!state.selectedNodeId || !state.cy) {
    elements.focusBadge.classList.add("hidden");
    elements.focusBadge.textContent = "";
    return;
  }
  const node = state.cy.getElementById(state.selectedNodeId);
  elements.focusBadge.classList.remove("hidden");
  elements.focusBadge.textContent = `Focused on ${node.data("entity")}: ${node.data("label")}`;
}

function isNodeCardOpen() {
  return !elements.nodeCard.classList.contains("hidden");
}

function resetNodeCardPosition() {
  elements.nodeCard.style.left = "";
  elements.nodeCard.style.top = "";
  elements.nodeCard.style.right = "32px";
}

function renderNodeCard(node) {
  const data = node.data();
  elements.nodeEntity.textContent = data.entity;
  elements.nodeLabel.textContent = data.label;
  elements.nodeMeta.textContent = `${data.table} - ${data.connections || 0} connections`;
  elements.nodeDetails.innerHTML = "";
  (data.details || []).forEach((item) => {
    const row = document.createElement("div");
    row.className = "detail-row";
    const key = document.createElement("span");
    key.className = "detail-key";
    key.textContent = item.field;
    const value = document.createElement("span");
    value.className = "detail-value";
    value.textContent = item.value;
    row.appendChild(key);
    row.appendChild(value);
    elements.nodeDetails.appendChild(row);
  });
}

export function moveHoverTooltip(event) {
  if (elements.hoverTooltip.classList.contains("hidden") || isNodeCardOpen()) {
    return;
  }
  const containerRect = elements.graphSection.getBoundingClientRect();
  const tooltipRect = elements.hoverTooltip.getBoundingClientRect();
  const nextLeft = clamp(event.clientX - containerRect.left + 16, 12, containerRect.width - tooltipRect.width - 12);
  const nextTop = clamp(event.clientY - containerRect.top + 16, 12, containerRect.height - tooltipRect.height - 12);
  elements.hoverTooltip.style.left = `${nextLeft}px`;
  elements.hoverTooltip.style.top = `${nextTop}px`;
}

export function hideHoverTooltip() {
  elements.hoverTooltip.classList.add("hidden");
}

export function showNodeCard(node, options = {}) {
  const { applyHighlight = true, preservePosition = false } = options;
  state.selectedNodeId = node.id();
  hideHoverTooltip();
  elements.nodeCard.classList.remove("hidden");
  if (!preservePosition && !elements.nodeCard.style.left && !elements.nodeCard.style.top) {
    resetNodeCardPosition();
  }
  renderNodeCard(node);
  if (applyHighlight) {
    applyProcessFlowHighlight(node);
  }
  updateFocusBadge();
}

export function hideNodeCard(options = {}) {
  const { clearHighlights = false } = options;
  state.selectedNodeId = null;
  elements.nodeCard.classList.add("hidden");
  if (state.cy && clearHighlights) {
    state.cy.elements().removeClass("dimmed highlighted search-hit search-edge");
  }
  updateFocusBadge();
}

export function showHoverTooltip(node, event) {
  if (!isHoverAllowed(node)) {
    hideHoverTooltip();
    return;
  }
  if (isNodeCardOpen()) {
    showNodeCard(node, { applyHighlight: false, preservePosition: true });
    hideHoverTooltip();
    return;
  }
  const data = node.data();
  const details = (data.details || [])
    .slice(0, 4)
    .map((item) => `<div class="hover-tooltip-row"><span>${item.field}</span><strong>${item.value}</strong></div>`)
    .join("");
  elements.hoverTooltip.innerHTML = `
    <div class="hover-tooltip-entity">${data.entity}</div>
    <div class="hover-tooltip-label">${data.label}</div>
    ${details}
  `;
  elements.hoverTooltip.classList.remove("hidden");
  moveHoverTooltip(event);
}

function startNodeCardDrag(event) {
  if (event.target === elements.closeNodeCard || event.target === elements.askAboutNode) {
    return;
  }
  const rect = elements.nodeCard.getBoundingClientRect();
  const containerRect = elements.graphSection.getBoundingClientRect();
  state.cardDrag.active = true;
  state.cardDrag.startX = event.clientX;
  state.cardDrag.startY = event.clientY;
  state.cardDrag.originLeft = rect.left - containerRect.left;
  state.cardDrag.originTop = rect.top - containerRect.top;
  elements.nodeCard.classList.add("dragging");
  event.preventDefault();
}

function moveNodeCard(event) {
  if (!state.cardDrag.active) {
    return;
  }
  const containerRect = elements.graphSection.getBoundingClientRect();
  const cardRect = elements.nodeCard.getBoundingClientRect();
  const nextLeft = state.cardDrag.originLeft + (event.clientX - state.cardDrag.startX);
  const nextTop = state.cardDrag.originTop + (event.clientY - state.cardDrag.startY);
  const maxLeft = Math.max(0, containerRect.width - cardRect.width - 16);
  const maxTop = Math.max(0, containerRect.height - cardRect.height - 16);
  elements.nodeCard.style.right = "auto";
  elements.nodeCard.style.left = `${clamp(nextLeft, 16, maxLeft)}px`;
  elements.nodeCard.style.top = `${clamp(nextTop, 16, maxTop)}px`;
}

function endNodeCardDrag() {
  if (!state.cardDrag.active) {
    return;
  }
  state.cardDrag.active = false;
  elements.nodeCard.classList.remove("dragging");
}

export function bindNodeCardEvents() {
  elements.closeNodeCard.addEventListener("click", hideNodeCard);
  elements.nodeCard.addEventListener("pointerdown", startNodeCardDrag);
  globalThis.addEventListener("pointermove", moveNodeCard);
  globalThis.addEventListener("pointerup", endNodeCardDrag);
}
