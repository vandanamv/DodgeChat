function createMessageHeader(role) {
  const head = document.createElement("div");
  head.className = `message-head ${role}`;

  const meta = document.createElement("div");
  meta.className = "message-meta";

  const name = document.createElement("div");
  name.className = "message-name";
  name.textContent = role === "assistant" ? "Dodge AI" : "You";
  meta.appendChild(name);

  if (role === "assistant") {
    const roleLabel = document.createElement("div");
    roleLabel.className = "message-role";
    roleLabel.textContent = "Graph Agent";
    meta.appendChild(roleLabel);
  }

  const avatar = document.createElement("div");
  avatar.className = `message-avatar ${role}`;
  avatar.textContent = role === "assistant" ? "D" : "U";

  if (role === "assistant") {
    head.appendChild(avatar);
    head.appendChild(meta);
  } else {
    head.appendChild(meta);
    head.appendChild(avatar);
  }

  return head;
}

function createEditActionButton(index, onEdit) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "message-action-button";
  button.setAttribute("aria-label", "Edit prompt");
  button.setAttribute("title", "Edit prompt");
  button.innerHTML = `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zm2.92 2.33H5v-.92l8.06-8.06.92.92L5.92 19.58zM20.71 7.04a1.003 1.003 0 0 0 0-1.42L18.37 3.29a1.003 1.003 0 0 0-1.42 0l-1.13 1.13 3.75 3.75 1.14-1.13z"></path>
    </svg>
  `;
  button.addEventListener("click", () => onEdit(index));
  return button;
}

export function createMessageElement(role, text, index, onEdit) {
  const wrapper = document.createElement("div");
  wrapper.className = `message ${role}`;

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.textContent = text;

  wrapper.appendChild(createMessageHeader(role));
  wrapper.appendChild(bubble);

  if (role === "user" && index > 0) {
    const actions = document.createElement("div");
    actions.className = "message-actions";
    actions.appendChild(createEditActionButton(index, onEdit));
    wrapper.appendChild(actions);
  }

  return wrapper;
}
