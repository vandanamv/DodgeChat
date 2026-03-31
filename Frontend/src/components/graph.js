import { elements, state } from "../core/state.js";
import { buildEntityGraphFromElements, compressGraphPositions } from "../lib/utils.js";
import {
  bindNodeCardEvents,
  hideHoverTooltip,
  hideNodeCard,
  moveHoverTooltip,
  showHoverTooltip,
  showNodeCard,
} from "./graph_features/node_card.js";
import {
  applyRelatedHighlights,
  focusGraphOnNodeIds,
  focusInitialGraph,
  isHoverAllowed,
  zoomGraph as zoomGraphInternal,
} from "./graph_features/graph_highlighting.js";
import { buildGraphStyles, buildClusteredGraphLayout } from "./graph_features/graph_layout.js";

function createGraphElements(graph) {
  return [
    ...graph.nodes.map((node) => ({ data: node, position: node.position })),
    ...graph.edges.map((edge) => ({ data: edge })),
  ];
}

function bindCytoscapeEvents() {
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

export function initGraph(graph) {
  const compactGraph = compressGraphPositions(buildClusteredGraphLayout(graph), 0.35);
  state.entityGraph = buildEntityGraphFromElements(compactGraph);
  state.cy = globalThis.cytoscape({
    container: elements.graphCanvas,
    elements: createGraphElements(compactGraph),
    style: buildGraphStyles(),
    layout: {
      name: "preset",
      fit: true,
      padding: 8,
    },
  });

  state.cy.ready(() => {
    focusInitialGraph();
  });

  bindCytoscapeEvents();
}

export function zoomGraph(multiplier) {
  zoomGraphInternal(multiplier, {
    x: elements.graphCanvas.clientWidth / 2,
    y: elements.graphCanvas.clientHeight / 2,
  });
}

export function bindGraphUiEvents() {
  bindNodeCardEvents();
}

export { applyRelatedHighlights, focusGraphOnNodeIds, hideNodeCard, showNodeCard };
