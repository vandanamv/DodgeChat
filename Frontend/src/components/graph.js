import { elements, state } from "../core/state.js";
import { PROCESS_FLOW } from "../lib/constants.js";
import { buildEntityGraphFromElements, clamp, compressGraphPositions } from "../lib/utils.js";

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

function isHoverAllowed(node) {
  if (!state.cy || !node || !node.length) {
    return false;
  }
  const searchHits = state.cy.nodes(".search-hit");
  if (searchHits.length > 0) {
    return node.hasClass("search-hit");
  }
  const highlighted = state.cy.nodes(".highlighted");
  if (highlighted.length > 0) {
    return node.hasClass("highlighted");
  }
  return true;
}

function moveHoverTooltip(event) {
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

function hideHoverTooltip() {
  elements.hoverTooltip.classList.add("hidden");
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

function flowNeighborsForNode(node) {
  const data = node.data();
  const flow = PROCESS_FLOW[data.entity];
  if (!flow) {
    return node.closedNeighborhood();
  }

  const preferredEntities = new Set(flow.next || []);
  const fallbackEntities = new Set(flow.previous || []);
  const directEdges = node.connectedEdges().filter((edge) => {
    const other = edge.source().id() === node.id() ? edge.target() : edge.source();
    return preferredEntities.has(other.data("entity"));
  });

  const edgesToUse = directEdges.length > 0
    ? directEdges
    : node.connectedEdges().filter((edge) => {
        const other = edge.source().id() === node.id() ? edge.target() : edge.source();
        return fallbackEntities.has(other.data("entity"));
      });

  if (!edgesToUse.length) {
    return node.closedNeighborhood();
  }

  const collection = state.cy.collection().merge(node);
  edgesToUse.forEach((edge) => {
    collection.merge(edge);
    collection.merge(edge.source());
    collection.merge(edge.target());
  });
  return collection;
}

function applyProcessFlowHighlight(node) {
  if (!state.cy) {
    return;
  }
  const flowCollection = flowNeighborsForNode(node);
  state.cy.elements().removeClass("dimmed highlighted search-edge");
  state.cy.elements().addClass("dimmed");
  flowCollection.removeClass("dimmed").addClass("highlighted");
  flowCollection.edges().addClass("search-edge");
  state.cy.animate({ fit: { eles: flowCollection, padding: 90 }, duration: 320 });
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
    state.cy.elements().removeClass("dimmed highlighted search-edge");
  }
  updateFocusBadge();
}

function showHoverTooltip(node, event) {
  if (isNodeCardOpen()) {
    showNodeCard(node, { applyHighlight: false, preservePosition: true });
    hideHoverTooltip();
    return;
  }
  if (!isHoverAllowed(node)) {
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

export function applyRelatedHighlights(nodeIds) {
  if (!state.cy) {
    return;
  }
  state.cy.elements().removeClass("search-hit search-edge");
  const nodeSet = new Set(nodeIds);
  nodeIds.forEach((id) => {
    state.cy.getElementById(id).addClass("search-hit");
  });
  state.cy.edges().forEach((edge) => {
    const sourceId = edge.data("source");
    const targetId = edge.data("target");
    if (nodeSet.has(sourceId) && nodeSet.has(targetId)) {
      edge.addClass("search-edge");
    }
  });
}

export function focusGraphOnNodeIds(nodeIds) {
  if (!state.cy || !Array.isArray(nodeIds) || nodeIds.length === 0) {
    return;
  }
  const nodes = nodeIds
    .map((id) => state.cy.getElementById(id))
    .filter((node) => node && node.length);
  if (nodes.length === 0) {
    return;
  }
  const collection = state.cy.collection();
  nodes.forEach((node) => collection.merge(node));
  state.cy.animate({ fit: { eles: collection, padding: 100 }, duration: 450 });
}

function focusInitialGraph() {
  if (!state.cy) {
    return;
  }
  state.cy.fit(undefined, 24);
  state.cy.zoom(Math.min(state.cy.zoom() * 1.14, state.cy.maxZoom()));
  state.cy.center();
}

export function zoomGraph(multiplier) {
  if (!state.cy) {
    return;
  }
  const currentZoom = state.cy.zoom();
  const nextZoom = clamp(currentZoom * multiplier, state.cy.minZoom(), state.cy.maxZoom());
  state.cy.animate({
    zoom: nextZoom,
    renderedPosition: {
      x: elements.graphCanvas.clientWidth / 2,
      y: elements.graphCanvas.clientHeight / 2,
    },
    duration: 180,
  });
}

function hubClusterGraph(graph, options = {}) {
  const nodeGroups = new Map();
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];

  nodes.forEach((node) => {
    const entity = String(node.entity || node.data?.entity || "").trim() || "other";
    if (!nodeGroups.has(entity)) {
      nodeGroups.set(entity, []);
    }
    nodeGroups.get(entity).push(node);
  });

  const spaceWidth = 1800;
  const spaceHeight = 1100;

  const palette = ["#6ab1ff", "#54c2a5", "#f29f7f", "#d76db2", "#f6d56a", "#717caa", "#49b5ff"];

  const getKeyColor = (key) => {
    if (!key || typeof key !== "string") {
      return palette[0];
    }
    let hash = 0;
    for (let i = 0; i < key.length; i += 1) {
      hash = (hash << 5) - hash + key.charCodeAt(i);
      hash |= 0;
    }
    return palette[Math.abs(hash) % palette.length];
  };

  return {
    ...graph,
    nodes: nodes.map((node) => {
      const posX = (Math.random() - 0.5) * spaceWidth;
      const posY = (Math.random() - 0.5) * spaceHeight;
      const categoryKey = String(node.entity || node.data?.entity || "unknown");
      const colorGroup = String(node.color || node.data?.color || getKeyColor(categoryKey));
      return {
        ...node,
        colorGroup,
        position: {
          x: Number(posX.toFixed(2)),
          y: Number(posY.toFixed(2)),
        },
      };
    }),
  }; 
}

export function initGraph(graph) {
  const compactGraph = compressGraphPositions(hubClusterGraph(graph), 0.35);
  state.entityGraph = buildEntityGraphFromElements(compactGraph);
  state.cy = globalThis.cytoscape({
    container: elements.graphCanvas,
    elements: [
      ...compactGraph.nodes.map((node) => ({ data: node, position: node.position })),
      ...compactGraph.edges.map((edge) => ({ data: edge })),
    ],
    style: [
      {
        selector: "node",
        style: {
          "background-color": "data(colorGroup)",
          "background-opacity": 0.9,
          label: "",
          color: "rgba(9, 64, 121, 1)",
          "font-family": "Plus Jakarta Sans, sans-serif",
          "font-size": 6,
          "text-wrap": "wrap",
          "text-max-width": 28,
          "text-valign": "center",
          "text-halign": "center",
          "text-margin-y": 0,
          "text-opacity": 0,
          width: "2.2",
          height: "2.2",
          "border-width": 0.9,
          "border-color": "data(colorGroup)",
          "border-opacity": 0.92,
          "shadow-blur": 0,
          "shadow-color": "rgba(0, 0, 0, 0)",
          "shadow-opacity": 0,
          opacity: 0.92,
          "z-index": 1,
        },
      },
      {
        selector: "edge",
        style: {
          width: "mapData(strength, 0, 1, 0.1, 0.45)",
          "line-color": "rgba(138, 187, 245, 0.04)",
          "curve-style": "bezier",
          "line-cap": "round",
          opacity: 0.04,
          "z-index": 0,
        },
      },
      {
        selector: ".granular-hidden",
        style: {
          display: "none",
        },
      },
      { selector: ".dimmed", style: { opacity: 0.24 } },
      { selector: ".highlighted", style: { "border-width": 1.8, "border-color": "#2f93ff", "shadow-blur": 6, "shadow-color": "rgba(47, 147, 255, 0.2)", "shadow-opacity": 0.22, opacity: 1, "text-opacity": 0.95 } },
      {
        selector: ".search-hit",
        style: {
          "background-color": "#ff9f79",
          "background-opacity": 0.95,
          "border-color": "#ff5a26",
          "border-width": 2.5,
          "shadow-blur": 12,
          "shadow-color": "rgba(255, 90, 38, 0.3)",
          "shadow-opacity": 0.32,
          "text-opacity": 1,
          width: "3.8",
          height: "3.8",
          opacity: 1,
          "z-index": 999,
        },
      },
      {
        selector: "node.highlighted",
        style: {
          "background-color": "#ffffff",
          "background-opacity": 0.7,
          "border-color": "#0ea5e9",
          "border-width": 2.6,
          "shadow-blur": 8,
          "shadow-color": "rgba(14, 165, 233, 0.38)",
          "shadow-opacity": 0.62,
          "text-opacity": 1,
          opacity: 1,
          "z-index": 999,
        },
      },
      { selector: "edge.highlighted", style: { "line-color": "rgba(255, 74, 44, 0.48)", width: 2.4, opacity: 0.92, "z-index": 998 } },
      { selector: ".search-edge", style: { "line-color": "rgba(255, 102, 51, 0.66)", width: 2.3, opacity: 0.92, "z-index": 997 } },
    ],
    layout: {
      name: "preset",
      fit: true,
      padding: 8,
    },
  });

  state.cy.ready(() => {
    focusInitialGraph();
  });

  state.cy.on("tap", "node", (event) => showNodeCard(event.target));
  state.cy.on("mouseover", "node", (event) => showHoverTooltip(event.target, event.originalEvent));
  state.cy.on("mousemove", "node", (event) => {
    if (!isHoverAllowed(event.target)) {
      hideHoverTooltip();
      return;
    }
    moveHoverTooltip(event.originalEvent);
  });
  state.cy.on("mouseout", "node", hideHoverTooltip);
  state.cy.on("tap", (event) => {
    if (event.target === state.cy) {
      hideNodeCard({ clearHighlights: true });
    }
  });
}

export function bindGraphUiEvents() {
  elements.closeNodeCard.addEventListener("click", hideNodeCard);
  elements.nodeCard.addEventListener("pointerdown", startNodeCardDrag);
  globalThis.addEventListener("pointermove", moveNodeCard);
  globalThis.addEventListener("pointerup", endNodeCardDrag);
}
