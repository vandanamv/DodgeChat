export const STORAGE_KEY = "dodgechat-history-v1";
export const INITIAL_ASSISTANT_MESSAGE = "Hi! I can help you analyze the Order to Cash process.";

export const PROCESS_FLOW = {
  Customer: { previous: ["Customer Company", "Customer Sales Area"], next: ["Sales Order"] },
  "Customer Sales Area": { previous: ["Customer"], next: ["Sales Order", "Plant"] },
  "Customer Company": { previous: ["Customer"], next: ["Sales Order"] },
  "Sales Order": { previous: ["Customer", "Customer Sales Area", "Customer Company"], next: ["Sales Order Item", "Schedule Line", "Delivery Item", "Delivery"] },
  "Sales Order Item": { previous: ["Sales Order"], next: ["Schedule Line", "Delivery Item", "Product Group", "Product", "Plant"] },
  "Schedule Line": { previous: ["Sales Order", "Sales Order Item"], next: ["Delivery Item"] },
  "Delivery Item": { previous: ["Sales Order", "Sales Order Item", "Schedule Line"], next: ["Delivery", "Billing Item", "Plant"] },
  Delivery: { previous: ["Delivery Item", "Sales Order"], next: ["Billing Item", "Billing Document"] },
  "Billing Item": { previous: ["Delivery", "Delivery Item", "Sales Order Item"], next: ["Billing Document", "Product"] },
  "Billing Document": { previous: ["Billing Item", "Delivery"], next: ["Billing Cancellation", "Journal Entry", "Payment"] },
  "Billing Cancellation": { previous: ["Billing Document"], next: ["Journal Entry"] },
  "Journal Entry": { previous: ["Billing Document", "Billing Cancellation"], next: ["Payment"] },
  Payment: { previous: ["Journal Entry", "Billing Document"], next: [] },
  "Product Group": { previous: ["Sales Order Item"], next: ["Product"] },
  Product: { previous: ["Product Group", "Sales Order Item", "Billing Item"], next: ["Product Description", "Product Plant", "Product Storage"] },
  "Product Description": { previous: ["Product"], next: [] },
  "Product Plant": { previous: ["Product"], next: ["Plant", "Product Storage"] },
  "Product Storage": { previous: ["Product", "Product Plant", "Sales Order Item", "Delivery Item"], next: ["Plant"] },
  Plant: { previous: ["Customer Sales Area", "Sales Order Item", "Delivery Item", "Product Plant", "Product Storage"], next: [] },
  "Customer Address": { previous: ["Customer"], next: [] },
};

export function buildProcessEntityGraph(flowMap) {
  const graph = new Map();
  Object.entries(flowMap).forEach(([entity, flow]) => {
    if (!graph.has(entity)) {
      graph.set(entity, new Set());
    }
    [...(flow.previous || []), ...(flow.next || [])].forEach((neighbor) => {
      if (!graph.has(neighbor)) {
        graph.set(neighbor, new Set());
      }
      graph.get(entity).add(neighbor);
      graph.get(neighbor).add(entity);
    });
  });
  return graph;
}

export const PROCESS_ENTITY_GRAPH = buildProcessEntityGraph(PROCESS_FLOW);
