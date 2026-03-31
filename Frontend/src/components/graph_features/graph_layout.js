export function buildClusteredGraphLayout(graph) {
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
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

export function buildGraphStyles() {
  return [
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
  ];
}
