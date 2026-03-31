import { state } from "../../core/state.js";
import { PROCESS_FLOW } from "../../lib/constants.js";
import { clamp } from "../../lib/utils.js";

function clearAllGraphEmphasis() {
  if (!state.cy) {
    return;
  }
  state.cy.elements().removeClass("dimmed highlighted search-hit search-edge");
}

export function isHoverAllowed(node) {
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

export function applyProcessFlowHighlight(node) {
  if (!state.cy) {
    return;
  }
  const flowCollection = flowNeighborsForNode(node);
  clearAllGraphEmphasis();
  state.cy.elements().addClass("dimmed");
  flowCollection.removeClass("dimmed").addClass("highlighted");
  flowCollection.edges().addClass("search-edge");
  state.cy.animate({ fit: { eles: flowCollection, padding: 90 }, duration: 320 });
}

export function applyRelatedHighlights(nodeIds) {
  if (!state.cy) {
    return;
  }
  clearAllGraphEmphasis();
  const nodeSet = new Set(nodeIds);
  state.cy.elements().addClass("dimmed");
  nodeIds.forEach((id) => {
    state.cy.getElementById(id).removeClass("dimmed").addClass("search-hit");
  });
  state.cy.edges().forEach((edge) => {
    const sourceId = edge.data("source");
    const targetId = edge.data("target");
    if (nodeSet.has(sourceId) && nodeSet.has(targetId)) {
      edge.removeClass("dimmed");
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

export function focusInitialGraph() {
  if (!state.cy) {
    return;
  }
  state.cy.fit(undefined, 24);
  state.cy.zoom(Math.min(state.cy.zoom() * 1.14, state.cy.maxZoom()));
  state.cy.center();
}

export function zoomGraph(multiplier, viewport) {
  if (!state.cy) {
    return;
  }
  const currentZoom = state.cy.zoom();
  const nextZoom = clamp(currentZoom * multiplier, state.cy.minZoom(), state.cy.maxZoom());
  state.cy.animate({
    zoom: nextZoom,
    renderedPosition: viewport,
    duration: 180,
  });
}
