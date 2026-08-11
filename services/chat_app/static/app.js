/* Argus chat client.
 *
 * Streaming uses fetch() + ReadableStream rather than EventSource, because
 * EventSource is GET-only and the message body has to be POSTed. The
 * AbortController that stops the stream is also what produces the
 * status="cancelled" inference log on the server side — the cancel feature and
 * the telemetry are the same mechanism.
 */

const $ = (id) => document.getElementById(id);

const el = {
  convList: $("conv-list"),
  convTitle: $("conv-title"),
  metaTurns: $("meta-turns"),
  messages: $("messages"),
  welcome: $("welcome"),
  input: $("input"),
  send: $("send"),
  stop: $("stop"),
  newChat: $("new-chat"),
  deleteChat: $("delete-chat"),
  note: $("status-note"),
};

const state = {
  conversationId: null,
  conversations: [],
  streaming: false,
  controller: null,
};

/* ---------- helpers ---------- */

async function api(path, options = {}) {
  const res = await fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error(detail.detail || `${res.status} ${res.statusText}`);
  }
  return res.status === 204 ? null : res.json();
}

function note(text, tone = "") {
  el.note.textContent = text;
  el.note.style.color = tone === "error" ? "var(--danger)" : "var(--text-faint)";
}

function atBottom() {
  const m = el.messages;
  return m.scrollHeight - m.scrollTop - m.clientHeight < 120;
}

function scrollToEnd(force = false) {
  if (force || atBottom()) el.messages.scrollTop = el.messages.scrollHeight;
}

/* ---------- rendering ---------- */

function addMessage(role, content = "") {
  el.welcome?.remove();

  const wrap = document.createElement("div");
  wrap.className = `msg ${role}`;

  const label = document.createElement("div");
  label.className = "msg-role";
  label.textContent = role === "user" ? "you" : "argus";

  const body = document.createElement("div");
  body.className = "msg-body";
  body.textContent = content;

  wrap.append(label, body);
  el.messages.append(wrap);
  scrollToEnd(true);
  return body;
}

function setTurns(n) {
  el.metaTurns.textContent = `${n} turn${n === 1 ? "" : "s"}`;
}

function renderConversations() {
  el.convList.replaceChildren();

  if (state.conversations.length === 0) {
    const p = document.createElement("p");
    p.className = "empty-note";
    p.textContent = "No conversations yet.";
    el.convList.append(p);
    return;
  }

  for (const conv of state.conversations) {
    const btn = document.createElement("button");
    btn.className = "conv-item" + (conv.id === state.conversationId ? " active" : "");
    btn.title = conv.title;

    const title = document.createElement("span");
    title.textContent = conv.title;

    const meta = document.createElement("span");
    meta.className = "conv-meta";
    meta.textContent = `${conv.message_count} msg · ${conv.id.slice(0, 8)}`;

    btn.append(title, meta);
    btn.addEventListener("click", () => openConversation(conv.id));
    el.convList.append(btn);
  }
}

/* ---------- conversation lifecycle ---------- */

async function refreshConversations() {
  state.conversations = await api("/conversations");
  renderConversations();
}

async function newConversation() {
  const conv = await api("/conversations", { method: "POST" });
  state.conversationId = conv.id;
  el.convTitle.textContent = conv.title;
  el.messages.replaceChildren();
  setTurns(0);
  el.deleteChat.hidden = false;
  await refreshConversations();
  el.input.focus();
  return conv.id;
}

async function openConversation(id) {
  if (state.streaming) stopStreaming();

  const conv = await api(`/conversations/${id}`);
  state.conversationId = conv.id;
  el.convTitle.textContent = conv.title;
  el.deleteChat.hidden = false;

  el.messages.replaceChildren();
  for (const m of conv.messages) addMessage(m.role, m.content);
  if (conv.messages.length === 0) note("Empty conversation — say something.");

  setTurns(conv.messages.filter((m) => m.role === "user").length);
  renderConversations();
  scrollToEnd(true);
}

async function deleteConversation() {
  if (!state.conversationId) return;
  if (state.streaming) stopStreaming();

  await api(`/conversations/${state.conversationId}`, { method: "DELETE" });
  state.conversationId = null;
  el.convTitle.textContent = "New conversation";
  el.messages.replaceChildren();
  el.deleteChat.hidden = true;
  setTurns(0);
  note("Conversation deleted.");
  await refreshConversations();
}

/* ---------- streaming ---------- */

function setStreaming(on) {
  state.streaming = on;
  el.send.hidden = on;
  el.stop.hidden = !on;
  el.input.disabled = on;
}

function stopStreaming() {
  // Abort the fetch. The server sees the disconnect and closes the generator,
  // which is what marks the inference as cancelled.
  state.controller?.abort();
  if (state.conversationId) {
    // Best-effort server-side cancel too, so a stream started in another tab stops.
    api(`/conversations/${state.conversationId}/cancel`, { method: "POST" }).catch(() => {});
  }
}

/** Parse an SSE byte stream into {event, data} frames. */
async function* sseFrames(response, signal) {
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";

  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += value;
      let split;
      while ((split = buffer.indexOf("\n\n")) !== -1) {
        const raw = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);

        let event = "message";
        const dataLines = [];
        for (const line of raw.split("\n")) {
          if (line.startsWith("event:")) event = line.slice(6).trim();
          else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
        }
        if (dataLines.length) {
          yield { event, data: JSON.parse(dataLines.join("\n")) };
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}

async function sendMessage(text) {
  if (!state.conversationId) await newConversation();

  addMessage("user", text);
  setTurns(el.messages.querySelectorAll(".msg.user").length);

  const body = addMessage("assistant", "");
  const cursor = document.createElement("span");
  cursor.className = "cursor";
  body.append(cursor);

  state.controller = new AbortController();
  setStreaming(true);
  note("streaming…");

  const started = performance.now();
  let tokens = 0;

  try {
    const res = await fetch(`/api/conversations/${state.conversationId}/messages`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content: text }),
      signal: state.controller.signal,
    });

    if (!res.ok) {
      const detail = await res.json().catch(() => ({}));
      throw new Error(detail.detail || `${res.status} ${res.statusText}`);
    }

    for await (const frame of sseFrames(res, state.controller.signal)) {
      if (frame.event === "token") {
        cursor.remove();
        body.append(document.createTextNode(frame.data.text));
        body.append(cursor);
        tokens += 1;
        scrollToEnd();
      } else if (frame.event === "error") {
        throw new Error(frame.data.message || "stream error");
      }
    }

    const ms = Math.round(performance.now() - started);
    note(`done · ${tokens} chunks · ${ms}ms`);
  } catch (err) {
    if (err.name === "AbortError") {
      const tag = document.createElement("span");
      tag.className = "msg-tag";
      tag.textContent = "cancelled";
      body.append(tag);
      note("cancelled — partial response kept");
    } else {
      note(err.message, "error");
    }
  } finally {
    cursor.remove();
    setStreaming(false);
    state.controller = null;
    await refreshConversations();
    el.input.focus();
  }
}

/* ---------- input ---------- */

function autoGrow() {
  el.input.style.height = "auto";
  el.input.style.height = `${Math.min(el.input.scrollHeight, 180)}px`;
}

function submit() {
  const text = el.input.value.trim();
  if (!text || state.streaming) return;
  el.input.value = "";
  autoGrow();
  sendMessage(text);
}

/* ---------- wiring ---------- */

el.input.addEventListener("input", autoGrow);
el.input.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    submit();
  }
});

el.send.addEventListener("click", submit);
el.stop.addEventListener("click", stopStreaming);
el.newChat.addEventListener("click", () => newConversation().then(() => note("")));
el.deleteChat.addEventListener("click", deleteConversation);

document.addEventListener("click", (e) => {
  const hint = e.target.closest(".hint");
  if (!hint) return;
  el.input.value = hint.dataset.prompt;
  autoGrow();
  submit();
});

refreshConversations()
  .then(() => el.input.focus())
  .catch((err) => note(err.message, "error"));
