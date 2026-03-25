import { elements } from "../core/state.js";
import { PROCESS_ENTITY_GRAPH } from "./constants.js";

export function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

export function setStatus(text, isError = false) {
  elements.statusBar.textContent = text;
  elements.statusBar.classList.toggle("error", isError);
}

export function buildEntityGraphFromElements(graph) {
  const nodeEntityById = new Map();
  const entityGraph = new Map();

  graph.nodes.forEach((node) => {
    const entity = node.entity;
    nodeEntityById.set(node.id, entity);
    if (!entityGraph.has(entity)) {
      entityGraph.set(entity, new Set());
    }
  });

  graph.edges.forEach((edge) => {
    const sourceEntity = nodeEntityById.get(edge.source);
    const targetEntity = nodeEntityById.get(edge.target);
    if (!sourceEntity || !targetEntity || sourceEntity === targetEntity) {
      return;
    }
    if (!entityGraph.has(sourceEntity)) {
      entityGraph.set(sourceEntity, new Set());
    }
    if (!entityGraph.has(targetEntity)) {
      entityGraph.set(targetEntity, new Set());
    }
    entityGraph.get(sourceEntity).add(targetEntity);
    entityGraph.get(targetEntity).add(sourceEntity);
  });

  PROCESS_ENTITY_GRAPH.forEach((neighbors, entity) => {
    if (!entityGraph.has(entity)) {
      entityGraph.set(entity, new Set());
    }
    neighbors.forEach((neighbor) => {
      if (!entityGraph.has(neighbor)) {
        entityGraph.set(neighbor, new Set());
      }
      entityGraph.get(entity).add(neighbor);
      entityGraph.get(neighbor).add(entity);
    });
  });

  return entityGraph;
}

export function compressGraphPositions(graph, factor = 0.72) {
  if (!graph || !Array.isArray(graph.nodes) || graph.nodes.length === 0) {
    return graph;
  }

  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;

  graph.nodes.forEach((node) => {
    const x = Number(node.position?.x || 0);
    const y = Number(node.position?.y || 0);
    minX = Math.min(minX, x);
    maxX = Math.max(maxX, x);
    minY = Math.min(minY, y);
    maxY = Math.max(maxY, y);
  });

  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;

  return {
    ...graph,
    nodes: graph.nodes.map((node) => {
      const x = Number(node.position?.x || 0);
      const y = Number(node.position?.y || 0);
      return {
        ...node,
        position: {
          x: Number(((x - centerX) * factor + centerX).toFixed(2)),
          y: Number(((y - centerY) * factor + centerY).toFixed(2)),
        },
      };
    }),
  };
}
