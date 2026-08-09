// Research Copilot frontend. Vanilla JS, no build step, no bundler.
// DOM is built with createElement/textContent only -- the HTML-injection
// property this deliberately avoids is checked for by scripts/check_api.py,
// which scans this file's source text.

const state = {
  user: null,
  topics: [],
  activeTopic: null,
  results: [],
  selectedPaper: null,
  collections: [],
  selectedCollectionId: null,
};

// --------------------------------------------------------------- api ----

async function apiGet(path) {
  const res = await fetch(path);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.message || body.error || `${res.status} ${res.statusText}`);
  return body;
}

async function apiSend(method, path, payload) {
  const res = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.message || body.error || `${res.status} ${res.statusText}`);
  return body;
}

// ------------------------------------------------------------- toasts ----

function toast(message, variant = "info") {
  const el = document.createElement("div");
  el.className = "toast" + (variant === "error" ? " toast--error" : "");
  const dot = document.createElement("span");
  dot.className = "toast__dot";
  const text = document.createElement("span");
  text.textContent = message;
  el.append(dot, text);
  document.getElementById("toasts").appendChild(el);
  setTimeout(() => el.remove(), 4500);
}

function clear(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
}

// -------------------------------------------------------------- tabs ----

function showTab(name) {
  for (const btn of document.querySelectorAll("#main-tabs .tab")) {
    btn.classList.toggle("is-active", btn.dataset.tab === name);
  }
  document.getElementById("view-paper").hidden = name !== "paper";
  document.getElementById("view-path").hidden = name !== "path";
  document.getElementById("view-agent").hidden = name !== "agent";
}

document.getElementById("main-tabs").addEventListener("click", (e) => {
  const btn = e.target.closest(".tab");
  if (btn) showTab(btn.dataset.tab);
});

// ------------------------------------------------------------- user ----

async function loadUser() {
  const chip = document.getElementById("user-chip").querySelector(".chip__label");
  try {
    const { email, authenticated } = await apiGet("/api/me");
    state.user = email;
    chip.textContent = authenticated ? email : "not signed in";
  } catch {
    chip.textContent = "unknown";
  }
}

// ------------------------------------------------------------ topics ----

async function loadTopics() {
  try {
    const { topics } = await apiGet("/api/topics");
    state.topics = topics;
    renderTopicFilters();
  } catch (err) {
    toast(`Could not load topics: ${err.message}`, "error");
  }
}

function renderTopicFilters() {
  const el = document.getElementById("topic-filters");
  clear(el);
  for (const t of state.topics) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "fchip" + (state.activeTopic === t.seed_topic ? " is-active" : "");
    const label = document.createElement("span");
    label.textContent = t.seed_topic || "(none)";
    const n = document.createElement("span");
    n.className = "fchip__n";
    n.textContent = t.paper_count;
    btn.append(label, n);
    btn.addEventListener("click", () => {
      state.activeTopic = state.activeTopic === t.seed_topic ? null : t.seed_topic;
      renderTopicFilters();
    });
    el.appendChild(btn);
  }
}

// ----------------------------------------------------------- search ----

document.getElementById("search-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const query = document.getElementById("search-query").value.trim();
  if (!query) return;
  const params = new URLSearchParams({ q: query, top_k: "20" });
  if (state.activeTopic) params.set("topic_field", state.activeTopic);
  try {
    const { results } = await apiGet(`/api/search?${params}`);
    state.results = results;
    renderResults();
  } catch (err) {
    toast(`Search failed: ${err.message}`, "error");
  }
});

function renderResults() {
  const list = document.getElementById("results-list");
  const empty = document.getElementById("results-empty");
  const count = document.getElementById("results-count");
  clear(list);
  count.textContent = state.results.length ? String(state.results.length) : "";
  empty.hidden = state.results.length > 0;

  const tmpl = document.getElementById("tmpl-paper-card");
  for (const paper of state.results) {
    const node = tmpl.content.firstElementChild.cloneNode(true);
    node.querySelector(".paper-card__title").textContent = paper.title;
    node.querySelector(".paper-card__year").textContent = paper.publication_year || "";
    node.querySelector(".paper-card__field").textContent = paper.primary_topic_field || "";
    node.querySelector(".paper-card__score").textContent = `sim ${paper.similarity.toFixed(2)}`;
    node.querySelector('[data-action="view"]').addEventListener("click", () => selectPaper(paper.work_id));
    node.querySelector('[data-action="add"]').addEventListener("click", () => promptAddToCollection(paper.work_id));
    list.appendChild(node);
  }
}

// ------------------------------------------------------------- paper ----

async function selectPaper(workId) {
  showTab("paper");
  try {
    const data = await apiGet(`/api/papers/${encodeURIComponent(workId)}`);
    state.selectedPaper = data;
    renderPaper();
  } catch (err) {
    toast(`Could not load paper: ${err.message}`, "error");
  }
}

function renderPaper() {
  const empty = document.getElementById("paper-empty");
  const content = document.getElementById("paper-content");
  const data = state.selectedPaper;
  if (!data) {
    empty.hidden = false;
    content.hidden = true;
    return;
  }
  empty.hidden = true;
  content.hidden = false;
  clear(content);

  const head = document.createElement("div");
  head.className = "detail__head";
  const title = document.createElement("div");
  title.className = "detail__title";
  title.textContent = data.paper.title;
  const meta = document.createElement("div");
  meta.className = "detail__meta";
  meta.append(
    metaSpan(data.paper.publication_year),
    metaSpan(data.paper.primary_topic_field),
    metaSpan(`${data.paper.cited_by_count} citations`),
    metaSpan(data.paper.is_oa ? "open access" : null),
  );
  head.append(title, meta);
  content.appendChild(head);

  const body = document.createElement("div");
  body.style.padding = "14px 18px";
  body.style.display = "flex";
  body.style.flexDirection = "column";
  body.style.gap = "12px";

  if (data.paper.narrative_abstract) {
    body.appendChild(sectionCard("Abstract", (el) => {
      const p = document.createElement("p");
      p.className = "detail__abstract";
      p.textContent = data.paper.narrative_abstract;
      el.appendChild(p);
    }));
  }

  body.appendChild(paperListCard("Read first (prerequisites)", data.prerequisites, selectPaper));
  body.appendChild(paperListCard("Builds on this (unlocks)", data.unlocks, selectPaper));
  body.appendChild(notesCard(data.paper.work_id));

  const addRow = document.createElement("div");
  const addBtn = document.createElement("button");
  addBtn.className = "btn btn--primary btn--sm";
  addBtn.textContent = "Add to a collection";
  addBtn.addEventListener("click", () => promptAddToCollection(data.paper.work_id));
  addRow.appendChild(addBtn);
  body.appendChild(addRow);

  content.appendChild(body);
}

function metaSpan(text) {
  const span = document.createElement("span");
  if (text) span.textContent = text;
  else span.hidden = true;
  return span;
}

function sectionCard(titleText, fill) {
  const card = document.createElement("div");
  card.className = "card";
  const head = document.createElement("div");
  head.className = "card__head";
  const title = document.createElement("span");
  title.className = "card__title";
  title.textContent = titleText;
  head.appendChild(title);
  card.appendChild(head);
  fill(card);
  return card;
}

function paperListCard(titleText, papers, onClick) {
  return sectionCard(titleText, (card) => {
    if (!papers || !papers.length) {
      const empty = document.createElement("div");
      empty.className = "card__empty";
      empty.textContent = "None in this corpus.";
      card.appendChild(empty);
      return;
    }
    const list = document.createElement("div");
    list.className = "reading-list";
    for (const p of papers) {
      const item = document.createElement("div");
      item.className = "reading-list__item";
      item.style.cursor = "pointer";
      const score = document.createElement("span");
      score.className = "reading-list__rank";
      score.textContent = (p.foundational_score || 0).toFixed(1);
      const title = document.createElement("span");
      title.textContent = p.title;
      item.append(score, title);
      item.addEventListener("click", () => onClick(p.work_id));
      list.appendChild(item);
    }
    card.appendChild(list);
  });
}

function notesCard(workId) {
  const card = document.createElement("div");
  card.className = "card";
  const head = document.createElement("div");
  head.className = "card__head";
  const title = document.createElement("span");
  title.className = "card__title";
  title.textContent = "Notes";
  head.appendChild(title);
  card.appendChild(head);

  const list = document.createElement("div");
  list.className = "notes-list";
  card.appendChild(list);

  const form = document.createElement("form");
  form.className = "form";
  form.style.marginTop = "10px";
  const textarea = document.createElement("textarea");
  textarea.placeholder = "Add a note on this paper…";
  const submit = document.createElement("button");
  submit.type = "submit";
  submit.className = "btn btn--ghost btn--sm";
  submit.textContent = "Save note";
  form.append(textarea, submit);
  card.appendChild(form);

  async function refresh() {
    clear(list);
    try {
      const { notes } = await apiGet(`/api/notes?work_id=${encodeURIComponent(workId)}`);
      for (const n of notes) {
        const item = document.createElement("div");
        item.className = "note";
        const text = document.createElement("div");
        text.textContent = n.note_text;
        const meta = document.createElement("div");
        meta.className = "note__meta";
        meta.textContent = new Date(n.created_at).toLocaleString();
        item.append(text, meta);
        list.appendChild(item);
      }
    } catch (err) {
      toast(`Could not load notes: ${err.message}`, "error");
    }
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const noteText = textarea.value.trim();
    if (!noteText) return;
    try {
      await apiSend("POST", "/api/notes", { work_id: workId, note_text: noteText });
      textarea.value = "";
      await refresh();
      toast("Note saved.");
    } catch (err) {
      toast(`Could not save note: ${err.message}`, "error");
    }
  });

  refresh();
  return card;
}

// -------------------------------------------------------- collections ----

async function loadCollections() {
  try {
    const { collections } = await apiGet("/api/collections");
    state.collections = collections;
    renderCollections();
  } catch (err) {
    toast(`Could not load collections: ${err.message}`, "error");
  }
}

function renderCollections() {
  const list = document.getElementById("collections-list");
  const empty = document.getElementById("collections-empty");
  clear(list);
  empty.hidden = state.collections.length > 0;
  for (const c of state.collections) {
    const row = document.createElement("div");
    row.className = "row" + (c.collection_id === state.selectedCollectionId ? " is-selected" : "");
    const title = document.createElement("div");
    title.className = "row__title";
    title.textContent = c.name;
    const meta = document.createElement("div");
    meta.className = "row__meta";
    meta.textContent = `${c.paper_count} paper${Number(c.paper_count) === 1 ? "" : "s"}`;
    row.append(title, meta);
    row.addEventListener("click", () => selectCollection(c.collection_id));
    list.appendChild(row);
  }
}

const newCollectionDialog = document.getElementById("new-collection-dialog");
const newCollectionInput = document.getElementById("new-collection-name");

document.getElementById("new-collection-btn").addEventListener("click", () => {
  newCollectionInput.value = "";
  newCollectionDialog.showModal();
  newCollectionInput.focus();
});

newCollectionDialog.querySelector('[data-action="cancel"]').addEventListener("click", () => newCollectionDialog.close());

newCollectionDialog.addEventListener("close", async () => {
  const name = newCollectionInput.value.trim();
  if (newCollectionDialog.returnValue !== "create" || !name) return;
  try {
    await apiSend("POST", "/api/collections", { name });
    await loadCollections();
    toast("Collection created.");
  } catch (err) {
    toast(`Could not create collection: ${err.message}`, "error");
  }
});

const addToCollectionDialog = document.getElementById("add-to-collection-dialog");
const addToCollectionList = document.getElementById("add-to-collection-list");

addToCollectionDialog.querySelector('[data-action="cancel"]').addEventListener("click", () => addToCollectionDialog.close());

function promptAddToCollection(workId) {
  if (!state.collections.length) {
    toast("Create a collection first (+ New, in the sidebar).", "error");
    return;
  }
  clear(addToCollectionList);
  for (const c of state.collections) {
    const row = document.createElement("button");
    row.type = "button";
    row.className = "dialog__row";
    row.textContent = c.name;
    row.addEventListener("click", async () => {
      addToCollectionDialog.close();
      try {
        await apiSend("POST", `/api/collections/${c.collection_id}/papers`, { work_id: workId });
        toast("Added to collection.");
        if (state.selectedCollectionId === c.collection_id) await selectCollection(c.collection_id);
      } catch (err) {
        toast(`Could not add paper: ${err.message}`, "error");
      }
    });
    addToCollectionList.appendChild(row);
  }
  addToCollectionDialog.showModal();
}

async function selectCollection(id) {
  state.selectedCollectionId = id;
  renderCollections();
  showTab("path");
  try {
    const { path } = await apiGet(`/api/collections/${id}/reading-path`);
    renderReadingPath(id, path);
  } catch (err) {
    toast(`Could not compute reading path: ${err.message}`, "error");
  }
}

function renderReadingPath(collectionId, path) {
  const empty = document.getElementById("path-empty");
  const content = document.getElementById("path-content");
  empty.hidden = true;
  content.hidden = false;
  clear(content);

  const toolbar = document.createElement("div");
  toolbar.style.display = "flex";
  toolbar.style.gap = "8px";
  toolbar.style.marginBottom = "4px";
  const saveBtn = document.createElement("button");
  saveBtn.className = "btn btn--primary btn--sm";
  saveBtn.textContent = "Save this order";
  saveBtn.addEventListener("click", async () => {
    try {
      await apiSend("POST", `/api/collections/${collectionId}/reading-path`);
      toast("Reading order saved.");
    } catch (err) {
      toast(`Could not save order: ${err.message}`, "error");
    }
  });
  toolbar.appendChild(saveBtn);
  content.appendChild(toolbar);

  if (!path.length) {
    const empty2 = document.createElement("p");
    empty2.className = "empty__text";
    empty2.textContent = "This collection has no papers yet -- add some from a search result.";
    content.appendChild(empty2);
    return;
  }

  content.appendChild(sectionCard("Dependency graph", (card) => card.appendChild(buildGraph(path))));

  content.appendChild(sectionCard("Reading order", (card) => {
    const list = document.createElement("div");
    list.className = "reading-list";
    path.forEach((entry, i) => {
      const item = document.createElement("div");
      item.className = "reading-list__item";
      const rank = document.createElement("span");
      rank.className = "reading-list__rank";
      rank.textContent = String(i + 1);
      const title = document.createElement("span");
      title.textContent = entry.title;
      item.append(rank, title);
      if (entry.in_citation_cycle) {
        const badge = document.createElement("span");
        badge.className = "badge badge--cycle";
        badge.textContent = "cycle";
        item.appendChild(badge);
      }
      item.style.cursor = "pointer";
      item.addEventListener("click", () => selectPaper(entry.work_id));
      list.appendChild(item);
    });
    card.appendChild(list);
  }));
}

// Grid layout ordered by reading rank (left-to-right, top-to-bottom) --
// not a full DAG layered layout, but every position and every edge drawn
// is real: rank comes from build_reading_path(), edges from citation_edges.
function buildGraph(path) {
  const wrap = document.createElement("div");
  wrap.className = "graph";

  const n = path.length;
  const cols = Math.max(1, Math.min(6, Math.ceil(Math.sqrt(n))));
  const xStep = 130, yStep = 70, margin = 50;
  const rows = Math.ceil(n / cols);
  const width = margin * 2 + (cols - 1) * xStep;
  const height = margin * 2 + (rows - 1) * yStep;

  const positions = {};
  path.forEach((entry, i) => {
    const col = i % cols, row = Math.floor(i / cols);
    positions[entry.work_id] = { x: margin + col * xStep, y: margin + row * yStep, rank: i, entry };
  });

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);

  const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
  const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
  marker.setAttribute("id", "rc-arrow");
  marker.setAttribute("viewBox", "0 0 10 10");
  marker.setAttribute("refX", "8");
  marker.setAttribute("refY", "5");
  marker.setAttribute("markerWidth", "6");
  marker.setAttribute("markerHeight", "6");
  marker.setAttribute("orient", "auto-start-reverse");
  const arrowPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  arrowPath.setAttribute("d", "M0,0 L10,5 L0,10 z");
  arrowPath.setAttribute("fill", "var(--tan-700)");
  marker.appendChild(arrowPath);
  defs.appendChild(marker);
  svg.appendChild(defs);

  for (const entry of path) {
    const from = positions[entry.work_id];
    for (const citedId of entry.prerequisites) {
      const to = positions[citedId];
      if (!to) continue;
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", from.x);
      line.setAttribute("y1", from.y);
      line.setAttribute("x2", to.x);
      line.setAttribute("y2", to.y);
      line.setAttribute("class", "graph__edge" + (entry.in_citation_cycle ? " graph__edge--cycle" : ""));
      svg.appendChild(line);
    }
  }

  for (const entry of path) {
    const pos = positions[entry.work_id];
    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("class", "graph__node" + (entry.in_citation_cycle ? " is-cycle" : ""));
    g.setAttribute("transform", `translate(${pos.x}, ${pos.y})`);

    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("r", "16");
    g.appendChild(circle);

    const rankText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    rankText.setAttribute("class", "rank");
    rankText.setAttribute("x", "0");
    rankText.setAttribute("y", "-22");
    rankText.setAttribute("text-anchor", "middle");
    rankText.textContent = String(pos.rank + 1);
    g.appendChild(rankText);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", "0");
    label.setAttribute("y", "30");
    label.setAttribute("text-anchor", "middle");
    label.textContent = truncate(entry.title, 18);
    g.appendChild(label);

    g.addEventListener("click", () => selectPaper(entry.work_id));
    svg.appendChild(g);
  }

  wrap.appendChild(svg);

  const legend = document.createElement("div");
  legend.className = "graph__legend";
  legend.append(
    legendItem("Number = reading rank (data-computed, not model-guessed)"),
    legendItem("Dashed gold edge = part of a citation cycle"),
  );
  wrap.appendChild(legend);

  return wrap;
}

function legendItem(text) {
  const span = document.createElement("span");
  span.textContent = text;
  return span;
}

function truncate(text, max) {
  return text.length > max ? text.slice(0, max - 1) + "…" : text;
}

// ------------------------------------------------------------ agent chat ----

document.getElementById("chat-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const input = document.getElementById("chat-input");
  const message = input.value.trim();
  if (!message) return;
  input.value = "";
  appendChatMessage(message, "user");
  try {
    const data = await apiSend("POST", "/api/agent/chat", { message });
    const reply = extractAgentReply(data.response);
    appendChatMessage(reply !== null ? reply : JSON.stringify(data.response), "agent");
  } catch (err) {
    appendChatMessage(err.message, "error");
  }
});

// Agent-serving responses vary by how the endpoint was built (ChatCompletions,
// MLflow ChatAgent, or the newer Responses-API shape) -- try each known shape
// and fall back to the raw response so a reply is never silently swallowed.
function extractAgentReply(response) {
  if (!response) return null;

  const choiceText = response.choices?.[0]?.message?.content;
  if (typeof choiceText === "string" && choiceText.trim()) return choiceText;

  if (Array.isArray(response.messages)) {
    for (let i = response.messages.length - 1; i >= 0; i--) {
      const m = response.messages[i];
      if (m?.role === "assistant" && typeof m.content === "string" && m.content.trim()) return m.content;
    }
  }

  if (Array.isArray(response.output)) {
    for (let i = response.output.length - 1; i >= 0; i--) {
      const item = response.output[i];
      if (item?.type === "message" && Array.isArray(item.content)) {
        const part = item.content.find((c) => typeof c?.text === "string" && c.text.trim());
        if (part) return part.text;
      }
    }
  }

  return null;
}

function appendChatMessage(text, variant) {
  const log = document.getElementById("chat-log");
  const msg = document.createElement("div");
  msg.className = `chat__msg chat__msg--${variant}`;
  msg.textContent = text;
  log.appendChild(msg);
  log.scrollTop = log.scrollHeight;
}

// ------------------------------------------------------------------ boot ----

loadUser();
loadTopics();
loadCollections();
