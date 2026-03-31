function createHistoryDeleteButton(sessionId, onDelete) {
  const button = document.createElement("button");
  button.className = "history-delete-button";
  button.type = "button";
  button.setAttribute("aria-label", "Delete chat");
  button.innerHTML = `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M9 3h6l1 2h5v2H3V5h5l1-2zm1 6h2v8h-2V9zm4 0h2v8h-2V9zM7 9h2v8H7V9zm1 12a2 2 0 0 1-2-2V8h12v11a2 2 0 0 1-2 2H8z"></path>
    </svg>
  `;
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    onDelete(sessionId);
  });
  return button;
}

export function createHistoryItemElement(session, previewText, onOpen, onDelete) {
  const item = document.createElement("div");
  item.className = "history-item";
  item.dataset.sessionId = session.id;

  const header = document.createElement("div");
  header.className = "history-item-header";

  const title = document.createElement("div");
  title.className = "history-item-title";
  title.textContent = session.title || "New chat";

  const meta = document.createElement("div");
  meta.className = "history-item-meta";
  meta.textContent = `${new Date(session.updatedAt).toLocaleString()} - ${session.history.length} messages`;

  const preview = document.createElement("div");
  preview.className = "history-item-preview";
  preview.textContent = previewText;

  header.appendChild(title);
  header.appendChild(createHistoryDeleteButton(session.id, onDelete));
  item.appendChild(header);
  item.appendChild(meta);
  item.appendChild(preview);
  item.addEventListener("click", () => onOpen(session.id));
  return item;
}

export function createHistoryEmptyElement() {
  const empty = document.createElement("div");
  empty.className = "history-empty";
  empty.textContent = "No saved chats yet.";
  return empty;
}
