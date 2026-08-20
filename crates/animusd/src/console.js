// AnimusDB Data Console — client-side app (ADR 0052). Vanilla JS, no
// bundler, no dependencies: this file and `console.html`/`console.css` are
// the whole client. Every screen is a pure client of the console's own
// `/console/api/*` JSON endpoints — nothing here ever renders a node id, a
// tablet, a replica, or anything else cluster-shaped, mirroring the
// server-side rule `console.rs`'s own module doc states.
//
// Routing mirrors the operator dashboard's own idiom
// (`dashboard_core.js::activateTab`): the server always returns the same
// static shell for every `/console/ui/*` path (`console::is_shell_path`), and
// this script reads `location.pathname` once on load to decide what to
// render into `#app`. A real `<a href>` is used for every navigation (the
// tables list ↔ a table's own page ↔ the create-table form) rather than a
// client-side push/pop-state router — the exception is the table page's own
// Config-tab jump nav (`#settings`/`#indexes`/`#danger`), a plain same-page
// in-document anchor, not a route.
(function () {
  "use strict";

  const app = document.getElementById("app");

  const esc = (s) =>
    String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  async function getJSON(path) {
    const res = await fetch(path, { headers: { Accept: "application/json" } });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `${path}: HTTP ${res.status}`);
    return data;
  }

  // POST/DELETE with a JSON body (POST) or none (DELETE) — the Config tab's
  // mutating calls. Mirrors `getJSON`'s "throw the server's own `error`
  // message when present" contract so every caller's `catch` shows the real
  // reason (a validation failure, a wire-path error) rather than a bare HTTP
  // status.
  async function sendJSON(method, path, body) {
    const res = await fetch(path, {
      method,
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `${path}: HTTP ${res.status}`);
    return data;
  }
  const postJSON = (path, body) => sendJSON("POST", path, body);
  const deleteJSON = (path) => sendJSON("DELETE", path);

  // Strip a trailing slash (except the bare root) so every route below has
  // exactly one spelling to match against.
  function normalizePath(p) {
    return p.length > 1 && p.endsWith("/") ? p.slice(0, -1) : p;
  }

  const TABLES_ROUTE_PREFIX = "/console/ui/tables/";
  const TABLE_LIST_ROUTES = new Set(["/", "/console", "/console/ui", "/console/ui/tables"]);

  // ---- the tables-list screen (this PR's real screen) --------------------

  let ALL_TABLES = [];
  let sortKey = "name";
  let sortDir = "asc";
  let query = "";

  function keyCell(key) {
    if (!key) return '<span class="dash">—</span>';
    return `<span class="key-name">${esc(key.name)}</span> <span class="key-type">(${esc(key.attribute_type)})</span>`;
  }

  function countCell(n) {
    return n == null ? '<span class="dash">—</span>' : String(n);
  }

  function boolCell(enabled, detail) {
    if (!enabled) return '<span class="bool-no">—</span>';
    const sub = detail ? `<span class="view-type">${esc(detail)}</span>` : "";
    return `<span class="bool-yes">✓</span>${sub}`;
  }

  function tableHref(name) {
    return TABLES_ROUTE_PREFIX + encodeURIComponent(name);
  }

  // Nulls (the LSI "structurally absent" case) always sort last, in either
  // direction — the point of the distinction is to set those rows apart,
  // which a plain ascending/descending flip would defeat.
  function compareValues(a, b, dir) {
    const an = a === null || a === undefined;
    const bn = b === null || b === undefined;
    if (an && bn) return 0;
    if (an) return 1;
    if (bn) return -1;
    const c = typeof a === "string" ? a.localeCompare(b) : a - b;
    return dir === "desc" ? -c : c;
  }

  const SORTERS = {
    name: (t) => t.name,
    pk: (t) => t.partition_key.name,
    sk: (t) => (t.sort_key ? t.sort_key.name : null),
    gsi: (t) => t.gsi_count,
    lsi: (t) => t.lsi_count,
    stream: (t) => t.stream.enabled,
    ttl: (t) => t.ttl.enabled,
  };

  const COLUMNS = [
    { key: "name", label: "Table" },
    { key: "pk", label: "Partition key" },
    { key: "sk", label: "Sort key" },
    { key: "gsi", label: "GSI" },
    { key: "lsi", label: "LSI" },
    { key: "stream", label: "Stream" },
    { key: "ttl", label: "TTL" },
  ];

  function visibleTables() {
    let rows = ALL_TABLES;
    if (query) {
      const q = query.toLowerCase();
      rows = rows.filter((t) => t.name.toLowerCase().includes(q));
    }
    const get = SORTERS[sortKey] || SORTERS.name;
    rows = rows.slice().sort((a, b) => compareValues(get(a), get(b), sortDir));
    return rows;
  }

  function renderTablesShell() {
    app.innerHTML = `
      <div class="view-head">
        <h1>Tables</h1>
        <span class="count" id="tbl-count"></span>
        <span class="spacer"></span>
        <a class="btn-new" href="${TABLES_ROUTE_PREFIX}new">+ New table</a>
      </div>
      <div class="toolbar">
        <input type="search" id="tbl-search" placeholder="Filter tables by name…" autocomplete="off">
      </div>
      <div class="tables-card">
        <div class="table-scroll">
          <table class="tables">
            <thead><tr id="tbl-head"></tr></thead>
            <tbody id="tbl-body"></tbody>
          </table>
        </div>
      </div>`;
    document.getElementById("tbl-search").addEventListener("input", (e) => {
      query = e.target.value;
      renderRows();
    });
    renderHead();
  }

  function renderHead() {
    const head = document.getElementById("tbl-head");
    head.innerHTML = COLUMNS.map((c) => {
      const sorted = c.key === sortKey;
      const arrow = sorted ? `<span class="arrow">${sortDir === "asc" ? "▲" : "▼"}</span>` : "";
      return `<th data-key="${c.key}" class="${sorted ? "sorted" : ""}">${c.label}${arrow}</th>`;
    }).join("");
    head.querySelectorAll("th").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        if (sortKey === key) sortDir = sortDir === "asc" ? "desc" : "asc";
        else {
          sortKey = key;
          sortDir = "asc";
        }
        renderHead();
        renderRows();
      });
    });
  }

  function renderRows() {
    const body = document.getElementById("tbl-body");
    const count = document.getElementById("tbl-count");
    if (ALL_TABLES.length === 0) {
      count.textContent = "";
      body.innerHTML = `<tr><td colspan="${COLUMNS.length}"><div class="empty-state">
        No tables yet.<div class="hint">Use “+ New table” above to create your first one.</div>
      </div></td></tr>`;
      return;
    }
    const rows = visibleTables();
    count.textContent =
      rows.length === ALL_TABLES.length ? `${ALL_TABLES.length} table${ALL_TABLES.length === 1 ? "" : "s"}` : `${rows.length} of ${ALL_TABLES.length}`;
    if (rows.length === 0) {
      body.innerHTML = `<tr><td colspan="${COLUMNS.length}"><div class="no-match">No tables match “${esc(query)}”.</div></td></tr>`;
      return;
    }
    body.innerHTML = rows
      .map(
        (t) => `
      <tr onclick="window.location.href='${tableHref(t.name)}'">
        <td class="name"><a href="${tableHref(t.name)}">${esc(t.name)}<span class="chevron">›</span></a></td>
        <td>${keyCell(t.partition_key)}</td>
        <td>${t.sort_key ? keyCell(t.sort_key) : '<span class="dash">—</span>'}</td>
        <td>${countCell(t.gsi_count)}</td>
        <td>${countCell(t.lsi_count)}</td>
        <td>${boolCell(t.stream.enabled, t.stream.view_type)}</td>
        <td>${boolCell(t.ttl.enabled, t.ttl.attribute_name)}</td>
      </tr>`
      )
      .join("");
  }

  async function renderTablesList() {
    renderTablesShell();
    document.getElementById("tbl-body").innerHTML =
      `<tr><td colspan="${COLUMNS.length}"><div class="loading">Loading tables…</div></td></tr>`;
    try {
      const data = await getJSON("/console/api/tables");
      ALL_TABLES = data.tables || [];
    } catch (e) {
      document.getElementById("tbl-body").innerHTML =
        `<tr><td colspan="${COLUMNS.length}"><div class="err-line">Couldn't load tables: ${esc(String(e))}</div></td></tr>`;
      return;
    }
    renderRows();
  }

  // ---- a not-yet-built screen (the create-table form, PR6) ----------------

  function renderStub(pathTail) {
    app.innerHTML = `
      <div class="stub">
        <h1>${esc(pathTail === "new" ? "Create table" : pathTail)}</h1>
        <p>The create-table form is not built yet.</p>
        <a class="back" href="/console/ui/tables">← Back to tables</a>
      </div>`;
  }

  // ---- the table page: Config tab (this PR) --------------------------------
  //
  // Three stacked sections — Settings, Indexes, Danger zone — under one
  // sticky jump nav, backed by the table-detail endpoint plus one mutating
  // endpoint per editable thing (`ttl`/`stream`/`gsi`/the table itself). The
  // whole page re-renders its own section after a successful mutation from
  // the server's own echoed response (never from the form's local guess of
  // what it just set), the same "state in, re-render" idiom the tables-list
  // screen above already uses for its own re-sorts/re-filters.

  const TABLE_API_PREFIX = "/console/api/tables/";
  const STREAM_VIEW_TYPES = ["NEW_AND_OLD_IMAGES", "NEW_IMAGE", "OLD_IMAGE", "KEYS_ONLY"];

  function tableApiPath(name, tail) {
    return TABLE_API_PREFIX + encodeURIComponent(name) + (tail ? `/${tail}` : "");
  }

  function keyLabel(k) {
    return `<span class="key-name">${esc(k.name)}</span> <span class="key-type">(${esc(k.attribute_type)})</span>`;
  }

  // An index key attribute's type is `null` whenever nothing in the catalog
  // recorded one (see `console.rs::IndexKeySummary` / issue #319) — render a
  // bare name then, never a made-up `(S)`.
  function indexKeyLabel(k) {
    return k.attribute_type
      ? keyLabel(k)
      : `<span class="key-name">${esc(k.name)}</span>`;
  }

  // A segmented control for one of DynamoDB's genuinely closed sets (the
  // stream view type) — never for a free-text attribute name (see the module
  // doc / `console.rs`'s own doc on why a picker would misrepresent the data
  // model there), and never for an attribute *type* on the Add-GSI form,
  // whose value this adapter cannot persist (issue #319).
  function segmented(field, options, selected) {
    return `<div class="segmented" data-field="${esc(field)}">${options
      .map(
        (o) =>
          `<button type="button" class="seg-opt${o === selected ? " selected" : ""}" data-value="${esc(o)}">${esc(o)}</button>`
      )
      .join("")}</div>`;
  }
  function segmentedValue(scope, field) {
    const el = scope.querySelector(`.segmented[data-field="${field}"] .seg-opt.selected`);
    return el ? el.dataset.value : null;
  }
  function wireSegmented(scope) {
    scope.querySelectorAll(".segmented").forEach((seg) => {
      seg.querySelectorAll(".seg-opt").forEach((btn) => {
        btn.addEventListener("click", () => {
          seg.querySelectorAll(".seg-opt").forEach((b) => b.classList.remove("selected"));
          btn.classList.add("selected");
        });
      });
    });
  }

  // A toggle switch for one of the two genuine booleans (TTL/stream
  // enabled) — a segmented ENABLED/DISABLED pair would be the wrong
  // affordance for a plain on/off (per this PR's own design brief).
  function toggleSwitch(field, on) {
    return `<button type="button" class="toggle-switch${on ? " on" : ""}" data-field="${esc(field)}" role="switch" aria-checked="${on}"><span class="knob"></span></button>`;
  }
  function toggleValue(scope, field) {
    const btn = scope.querySelector(`.toggle-switch[data-field="${field}"]`);
    return btn ? btn.classList.contains("on") : false;
  }
  function wireToggles(scope) {
    scope.querySelectorAll(".toggle-switch").forEach((btn) => {
      btn.addEventListener("click", () => {
        const on = !btn.classList.contains("on");
        btn.classList.toggle("on", on);
        btn.setAttribute("aria-checked", String(on));
      });
    });
  }

  function statusPill(status) {
    const cls =
      status === "ACTIVE" ? "pill-active" : status === "CREATING" ? "pill-creating" : "pill-deleting";
    return `<span class="status-pill ${cls}">${esc(status)}</span>`;
  }

  let TABLE_DETAIL = null;

  async function renderTablePage(name) {
    app.innerHTML = `<p class="loading">Loading ${esc(name)}…</p>`;
    try {
      TABLE_DETAIL = await getJSON(tableApiPath(name));
    } catch (e) {
      app.innerHTML = `
        <div class="stub">
          <h1>${esc(name)}</h1>
          <p class="err-line">Couldn't load this table: ${esc(String(e.message || e))}</p>
          <a class="back" href="/console/ui/tables">← Back to tables</a>
        </div>`;
      return;
    }
    app.innerHTML = `
      <div class="table-page">
        <div class="view-head">
          <a class="back-link" href="/console/ui/tables">← Tables</a>
          <h1>${esc(TABLE_DETAIL.name)}</h1>
        </div>
        <nav class="jump-nav">
          <a href="#settings">Settings</a>
          <a href="#indexes">Indexes</a>
          <a href="#danger">Danger zone</a>
        </nav>
        <section id="settings" class="config-section"></section>
        <section id="indexes" class="config-section"></section>
        <section id="danger" class="config-section"></section>
      </div>`;
    renderSettingsSection();
    renderIndexesSection();
    renderDangerSection();
  }

  // -- Settings: the key-schema fact strip + the two editable things -------

  function renderSettingsSection() {
    const d = TABLE_DETAIL;
    const el = document.getElementById("settings");
    el.innerHTML = `
      <h2>Settings</h2>
      <div class="fact-strip">
        <div class="fact"><span class="fact-label">Table name</span><span class="fact-value mono">${esc(d.name)}</span></div>
        <div class="fact"><span class="fact-label">Partition key</span><span class="fact-value">${keyLabel(d.partition_key)}</span></div>
        <div class="fact"><span class="fact-label">Sort key</span><span class="fact-value">${d.sort_key ? keyLabel(d.sort_key) : '<span class="dash">—</span>'}</span></div>
      </div>
      <p class="fixed-note">A table's key schema is fixed at creation and can't be changed afterward.</p>

      <div class="config-row">
        <div class="config-row-head">
          <h3>TTL</h3>
          <button type="button" class="btn-edit" data-edit="ttl">Edit</button>
        </div>
        <div class="config-row-view" data-view="ttl">${
          d.ttl.enabled
            ? `<span class="bool-yes">Enabled</span> — expires items via <code>${esc(d.ttl.attribute_name)}</code>`
            : '<span class="bool-no">Disabled</span>'
        }</div>
        <div class="config-row-edit hidden" data-edit-form="ttl">
          <label class="field-row">${toggleSwitch("ttl-enabled", d.ttl.enabled)}<span>Enabled</span></label>
          <label class="field">Attribute name
            <input type="text" class="attr-input" id="ttl-attr" placeholder="e.g. expiresAt" autocomplete="off" value="${esc(d.ttl.attribute_name || "")}">
          </label>
          <p class="field-hint">An absolute Unix epoch second (not milliseconds) — items past this instant become eligible for deletion.</p>
          <div class="edit-actions">
            <button type="button" class="btn-save" data-save="ttl">Save</button>
            <button type="button" class="btn-cancel" data-cancel="ttl">Cancel</button>
          </div>
          <p class="err-line hidden" data-err="ttl"></p>
        </div>
      </div>

      <div class="config-row">
        <div class="config-row-head">
          <h3>Stream</h3>
          <button type="button" class="btn-edit" data-edit="stream">Edit</button>
        </div>
        <div class="config-row-view" data-view="stream">${
          d.stream.enabled
            ? `<span class="bool-yes">Enabled</span> — <span class="view-type">${esc(d.stream.view_type)}</span>`
            : '<span class="bool-no">Disabled</span>'
        }</div>
        <div class="config-row-edit hidden" data-edit-form="stream">
          <label class="field-row">${toggleSwitch("stream-enabled", d.stream.enabled)}<span>Enabled</span></label>
          <div class="field">View type${segmented(
            "stream-view-type",
            STREAM_VIEW_TYPES,
            d.stream.view_type || STREAM_VIEW_TYPES[0]
          )}</div>
          <div class="edit-actions">
            <button type="button" class="btn-save" data-save="stream">Save</button>
            <button type="button" class="btn-cancel" data-cancel="stream">Cancel</button>
          </div>
          <p class="err-line hidden" data-err="stream"></p>
        </div>
      </div>`;

    wireSegmented(el);
    wireToggles(el);
    wireEditableRow(el, "ttl", saveTtl);
    wireEditableRow(el, "stream", saveStream);
  }

  function wireEditableRow(scope, key, onSave) {
    scope.querySelector(`[data-edit="${key}"]`).addEventListener("click", () => setEditing(scope, key, true));
    scope.querySelector(`[data-cancel="${key}"]`).addEventListener("click", () => setEditing(scope, key, false));
    scope.querySelector(`[data-save="${key}"]`).addEventListener("click", () => onSave(scope, key));
  }

  function setEditing(scope, key, editing) {
    scope.querySelector(`[data-view="${key}"]`).classList.toggle("hidden", editing);
    scope.querySelector(`[data-edit-form="${key}"]`).classList.toggle("hidden", !editing);
    scope.querySelector(`[data-edit="${key}"]`).classList.toggle("hidden", editing);
    if (!editing) scope.querySelector(`[data-err="${key}"]`).classList.add("hidden");
  }

  async function saveTtl(scope, key) {
    const errEl = scope.querySelector(`[data-err="${key}"]`);
    const saveBtn = scope.querySelector(`[data-save="${key}"]`);
    const enabled = toggleValue(scope, "ttl-enabled");
    const attribute = scope.querySelector("#ttl-attr").value.trim();
    errEl.classList.add("hidden");
    if (!attribute) {
      errEl.textContent = "Attribute name is required.";
      errEl.classList.remove("hidden");
      return;
    }
    saveBtn.disabled = true;
    try {
      const resp = await postJSON(tableApiPath(TABLE_DETAIL.name, "ttl"), {
        enabled,
        attribute_name: attribute,
      });
      TABLE_DETAIL.ttl = resp.ttl;
      renderSettingsSection();
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
      saveBtn.disabled = false;
    }
  }

  async function saveStream(scope, key) {
    const errEl = scope.querySelector(`[data-err="${key}"]`);
    const saveBtn = scope.querySelector(`[data-save="${key}"]`);
    const enabled = toggleValue(scope, "stream-enabled");
    const viewType = segmentedValue(scope, "stream-view-type");
    errEl.classList.add("hidden");
    saveBtn.disabled = true;
    try {
      const payload = enabled ? { enabled: true, view_type: viewType } : { enabled: false };
      const resp = await postJSON(tableApiPath(TABLE_DETAIL.name, "stream"), payload);
      TABLE_DETAIL.stream = resp.stream;
      renderSettingsSection();
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
      saveBtn.disabled = false;
    }
  }

  // -- Indexes: GSIs (addable/droppable) then LSIs (create-time-only) ------

  function gsiRowHtml(g) {
    return `
      <div class="index-row">
        <div class="index-row-main">
          <span class="index-name">${esc(g.name)}</span>
          ${statusPill(g.status)}
        </div>
        <div class="index-row-keys">${indexKeyLabel(g.hash_attribute)}${
          g.sort_attribute ? ` / ${indexKeyLabel(g.sort_attribute)}` : ""
        }</div>
        <button type="button" class="btn-drop" data-drop-gsi="${esc(g.name)}"${
          g.status === "DELETING" ? " disabled" : ""
        }>Drop</button>
      </div>`;
  }

  function lsiRowHtml(l) {
    return `
      <div class="index-row index-row-lsi">
        <div class="index-row-main"><span class="index-name">${esc(l.name)}</span></div>
        <div class="index-row-keys">${indexKeyLabel(l.sort_attribute)}</div>
      </div>`;
  }

  function renderIndexesSection() {
    const d = TABLE_DETAIL;
    const el = document.getElementById("indexes");
    el.innerHTML = `
      <h2>Indexes</h2>
      <h3 class="subhead">Global secondary indexes</h3>
      <p class="section-note">A GSI is its own materialized table — its own hash key, backfilled asynchronously.</p>
      <div class="index-card">
        <div class="index-list">${
          d.gsis.length
            ? d.gsis.map(gsiRowHtml).join("")
            : '<div class="empty-state">No global secondary indexes.</div>'
        }</div>
        <button type="button" class="btn-new" id="btn-add-gsi">+ Add GSI</button>
        <div class="gsi-form hidden" id="gsi-form">
          <label class="field">Index name
            <input type="text" class="attr-input" id="gsi-name" placeholder="e.g. by-status" autocomplete="off">
          </label>
          <label class="field">Hash attribute
            <input type="text" class="attr-input" id="gsi-hash-attr" placeholder="attribute name" autocomplete="off">
          </label>
          <label class="field">Sort attribute (optional)
            <input type="text" class="attr-input" id="gsi-sort-attr" placeholder="attribute name" autocomplete="off">
          </label>
          <div class="edit-actions">
            <button type="button" class="btn-save" id="gsi-save">Add index</button>
            <button type="button" class="btn-cancel" id="gsi-cancel">Cancel</button>
          </div>
          <p class="err-line hidden" id="gsi-err"></p>
        </div>
      </div>

      <h3 class="subhead">Local secondary indexes</h3>
      <p class="section-note">An LSI is a scope inside the table's own storage, not a separate table. LSIs are declared when a table is created and can't be added or dropped afterward.</p>
      <div class="index-card">
        <div class="index-list">${
          d.lsis.length
            ? d.lsis.map(lsiRowHtml).join("")
            : '<div class="empty-state">No local secondary indexes.</div>'
        }</div>
      </div>`;

    wireSegmented(el);
    el.querySelectorAll("[data-drop-gsi]").forEach((btn) => {
      btn.addEventListener("click", () => dropGsi(btn.dataset.dropGsi, btn));
    });
    const addBtn = el.querySelector("#btn-add-gsi");
    const form = el.querySelector("#gsi-form");
    addBtn.addEventListener("click", () => {
      addBtn.classList.add("hidden");
      form.classList.remove("hidden");
    });
    el.querySelector("#gsi-cancel").addEventListener("click", () => {
      form.classList.add("hidden");
      addBtn.classList.remove("hidden");
      el.querySelector("#gsi-err").classList.add("hidden");
    });
    el.querySelector("#gsi-save").addEventListener("click", () => saveGsi(el));
  }

  async function saveGsi(scope) {
    const errEl = scope.querySelector("#gsi-err");
    errEl.classList.add("hidden");
    const indexName = scope.querySelector("#gsi-name").value.trim();
    const hashAttribute = scope.querySelector("#gsi-hash-attr").value.trim();
    const sortAttribute = scope.querySelector("#gsi-sort-attr").value.trim();
    if (!indexName || !hashAttribute) {
      errEl.textContent = "Index name and hash attribute are both required.";
      errEl.classList.remove("hidden");
      return;
    }
    // Names only — a GSI's key attribute carries no durable type on this
    // adapter's `UpdateTable` path (issue #319), so the form asks for none.
    const req = { index_name: indexName, hash_attribute: hashAttribute };
    if (sortAttribute) {
      req.sort_attribute = sortAttribute;
    }
    const saveBtn = scope.querySelector("#gsi-save");
    saveBtn.disabled = true;
    try {
      const resp = await postJSON(tableApiPath(TABLE_DETAIL.name, "gsi"), req);
      TABLE_DETAIL.gsis.push(resp.gsi);
      renderIndexesSection();
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
      saveBtn.disabled = false;
    }
  }

  async function dropGsi(name, btn) {
    if (!window.confirm(`Drop index “${name}”? This deletes the index's materialized data — it cannot be undone.`)) {
      return;
    }
    btn.disabled = true;
    try {
      await deleteJSON(tableApiPath(TABLE_DETAIL.name, `gsi/${encodeURIComponent(name)}`));
      TABLE_DETAIL.gsis = TABLE_DETAIL.gsis.filter((g) => g.name !== name);
      renderIndexesSection();
    } catch (e) {
      btn.disabled = false;
      window.alert(`Couldn't drop index “${name}”: ${e.message || e}`);
    }
  }

  // -- Danger zone: delete the table, with a typed-name confirm step -------

  function renderDangerSection() {
    const d = TABLE_DETAIL;
    const el = document.getElementById("danger");
    el.innerHTML = `
      <h2>Danger zone</h2>
      <div class="danger-card">
        <div class="danger-row">
          <div>
            <div class="danger-title">Delete this table</div>
            <div class="danger-body">Permanently deletes <code>${esc(d.name)}</code> and all of its items, indexes, and stream data. This can't be undone.</div>
          </div>
          <button type="button" class="btn-danger" id="btn-delete-table">Delete table</button>
        </div>
        <div class="danger-confirm hidden" id="delete-confirm">
          <label class="field">Type <code>${esc(d.name)}</code> to confirm
            <input type="text" class="attr-input" id="delete-confirm-input" autocomplete="off">
          </label>
          <div class="edit-actions">
            <button type="button" class="btn-danger" id="btn-delete-confirm" disabled>Delete permanently</button>
            <button type="button" class="btn-cancel" id="btn-delete-cancel">Cancel</button>
          </div>
          <p class="err-line hidden" id="delete-err"></p>
        </div>
      </div>`;

    const deleteBtn = el.querySelector("#btn-delete-table");
    const confirmBox = el.querySelector("#delete-confirm");
    const input = el.querySelector("#delete-confirm-input");
    const confirmBtn = el.querySelector("#btn-delete-confirm");
    const errEl = el.querySelector("#delete-err");

    deleteBtn.addEventListener("click", () => {
      deleteBtn.classList.add("hidden");
      confirmBox.classList.remove("hidden");
      input.focus();
    });
    el.querySelector("#btn-delete-cancel").addEventListener("click", () => {
      confirmBox.classList.add("hidden");
      deleteBtn.classList.remove("hidden");
      input.value = "";
      confirmBtn.disabled = true;
      errEl.classList.add("hidden");
    });
    input.addEventListener("input", () => {
      confirmBtn.disabled = input.value !== d.name;
    });
    confirmBtn.addEventListener("click", async () => {
      confirmBtn.disabled = true;
      errEl.classList.add("hidden");
      try {
        await deleteJSON(tableApiPath(d.name));
        window.location.href = "/console/ui/tables";
      } catch (e) {
        errEl.textContent = String(e.message || e);
        errEl.classList.remove("hidden");
        confirmBtn.disabled = false;
      }
    });
  }

  // ---- routing -------------------------------------------------------------

  const path = normalizePath(window.location.pathname);
  if (path.startsWith(TABLES_ROUTE_PREFIX)) {
    const tail = decodeURIComponent(path.slice(TABLES_ROUTE_PREFIX.length));
    if (tail === "new") {
      renderStub(tail);
    } else {
      renderTablePage(tail);
    }
  } else if (TABLE_LIST_ROUTES.has(path)) {
    renderTablesList();
  } else {
    // Any other `/console/ui/*` deep link the shell serves but this router
    // doesn't yet recognize — the tables list is the console's landing
    // screen, so fall back to it rather than a bare blank page.
    renderTablesList();
  }
})();
