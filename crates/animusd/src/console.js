// animusd console (ADR 0052's "AnimusDB Data Console") — client-side app. Vanilla JS, no
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

  // The table page's Items and Stream data tabs each have their own real
  // URL, one path segment past the table's own (`.../tables/{name}/items`,
  // `.../tables/{name}/stream`) — see the table-page routing section's own
  // comment for why this, rather than a shared-page pushState-driven tab
  // strip (`dashboard_core.js::activateTab`'s idiom), is the tab-switch
  // mechanism here.
  function tableItemsHref(name) {
    return tableHref(name) + "/items";
  }
  function tableStreamHref(name) {
    return tableHref(name) + "/stream";
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

  // ---- the create-table form ------------------------------------------------
  //
  // The console's last screen: table name, partition key (name + a real
  // S/N/B control — `CreateTable` genuinely records a base-table key's
  // type), an optional sort key (same shape), any LSIs (declarable **only**
  // here — see `console.rs::CreateLsiRequest`'s own doc for why: DynamoDB
  // LSIs are create-time-only), any GSIs (name, hash [+ sort] attribute,
  // projection), a stream, and TTL. Every attribute *name* stays free text
  // (the console's standing rule); every index key attribute stays
  // name-only, no type control — a deliberate scope cut for *this form*,
  // not a mechanism gap: `CreateTable` genuinely records a real type for an
  // index key attribute now (issue #319, closed), this form just doesn't
  // collect one (see `console.rs::CreateTableRequest`'s own doc). The
  // Config tab's Add-GSI form below (`saveGsi`) *does* offer one, since
  // that path's own gap is what #319 was filed for.
  //
  // The sort-key toggle defaults **on** (not off): an earlier draft of this
  // form gated the LSI section on the sort key being present — correct,
  // since an LSI needs one — but defaulted the toggle off, so a blank form
  // opened with the LSI section permanently blocked and no visible way to
  // unblock it. Defaulting the toggle on means a blank form always has a
  // live path to declaring an LSI; if a user later switches it off with
  // LSI rows still present, the section's own blocked message still points
  // straight back at the (still-visible, still-live) switch above it.

  function keyAttrFieldHtml(prefix, label, nameVal, typeVal) {
    return `
      <div class="field-row-2">
        <label class="field">${esc(label)} attribute name
          <input type="text" class="attr-input" id="${prefix}-name" placeholder="attribute name" autocomplete="off" value="${esc(nameVal)}">
        </label>
        <label class="field">Type
          <select class="kv-type" id="${prefix}-type">
            <option value="S"${typeVal === "S" ? " selected" : ""}>S</option>
            <option value="N"${typeVal === "N" ? " selected" : ""}>N</option>
            <option value="B"${typeVal === "B" ? " selected" : ""}>B</option>
          </select>
        </label>
      </div>`;
  }

  function lsiFormRowHtml() {
    return `
      <div class="create-index-row" data-lsi-row>
        <input type="text" class="attr-input" placeholder="index name" data-lsi-name autocomplete="off">
        <input type="text" class="attr-input" placeholder="sort key attribute name" data-lsi-sort autocomplete="off">
        <button type="button" class="btn-drop" data-remove-row>Remove</button>
      </div>`;
  }

  const PROJECTION_TYPES = ["ALL", "KEYS_ONLY", "INCLUDE"];

  function gsiFormRowHtml() {
    return `
      <div class="create-index-row create-index-row-gsi" data-gsi-row>
        <div class="create-index-row-fields">
          <input type="text" class="attr-input" placeholder="index name" data-gsi-name autocomplete="off">
          <input type="text" class="attr-input" placeholder="hash attribute name" data-gsi-hash autocomplete="off">
          <input type="text" class="attr-input" placeholder="sort attribute name (optional)" data-gsi-sort autocomplete="off">
        </div>
        <div class="create-index-row-fields">
          <div class="field">Projection${segmented("gsi-projection-type", PROJECTION_TYPES, "ALL")}</div>
          <input type="text" class="attr-input gsi-nonkey hidden" placeholder="non-key attributes, comma-separated" data-gsi-nonkey autocomplete="off">
        </div>
        <button type="button" class="btn-drop" data-remove-row>Remove</button>
      </div>`;
  }

  function wireGsiRow(row) {
    wireSegmented(row);
    const nonKeyInput = row.querySelector(".gsi-nonkey");
    row.querySelectorAll(`.segmented[data-field="gsi-projection-type"] .seg-opt`).forEach((btn) => {
      btn.addEventListener("click", () => {
        nonKeyInput.classList.toggle("hidden", btn.dataset.value !== "INCLUDE");
      });
    });
  }

  function renderCreateTableForm() {
    app.innerHTML = `
      <div class="table-page create-table-page">
        <div class="view-head">
          <a class="back-link" href="/console/ui/tables">← Tables</a>
          <h1>Create table</h1>
        </div>
        <form class="create-form" id="create-table-form">
          <section class="config-section">
            <h2>Table</h2>
            <label class="field">Table name
              <input type="text" class="attr-input" id="ct-name" placeholder="e.g. orders" autocomplete="off">
            </label>
          </section>

          <section class="config-section">
            <h2>Partition key</h2>
            ${keyAttrFieldHtml("ct-pk", "Partition key", "", "S")}
          </section>

          <section class="config-section">
            <h2>Sort key</h2>
            <label class="field-row">${toggleSwitch("ct-sk-enabled", true)}<span>Table has a sort key</span></label>
            <div id="ct-sk-fields">${keyAttrFieldHtml("ct-sk", "Sort key", "", "S")}</div>
          </section>

          <section class="config-section">
            <h2>Local secondary indexes</h2>
            <p class="section-note">An LSI shares the table's own partition key and adds an alternate sort key. Declarable only here — DynamoDB never allows adding or dropping an LSI after a table is created.</p>
            <div class="index-list" id="ct-lsi-rows"></div>
            <button type="button" class="btn-new" id="ct-add-lsi">+ Add LSI</button>
            <p class="field-hint hidden" id="ct-lsi-blocked">Turn on “Table has a sort key” above to declare a local secondary index.</p>
          </section>

          <section class="config-section">
            <h2>Global secondary indexes</h2>
            <p class="section-note">A GSI is its own hash key (plus an optional sort key), backfilled asynchronously once the table exists.</p>
            <div class="index-list" id="ct-gsi-rows"></div>
            <button type="button" class="btn-new" id="ct-add-gsi">+ Add GSI</button>
          </section>

          <section class="config-section">
            <h2>Stream</h2>
            <label class="field-row">${toggleSwitch("ct-stream-enabled", false)}<span>Enabled</span></label>
            <div class="field hidden" id="ct-stream-view-field">View type${segmented("ct-stream-view-type", STREAM_VIEW_TYPES, STREAM_VIEW_TYPES[0])}</div>
          </section>

          <section class="config-section">
            <h2>TTL</h2>
            <label class="field-row">${toggleSwitch("ct-ttl-enabled", false)}<span>Enabled</span></label>
            <label class="field hidden" id="ct-ttl-attr-field">Attribute name
              <input type="text" class="attr-input" id="ct-ttl-attr" placeholder="e.g. expiresAt" autocomplete="off">
            </label>
          </section>

          <div class="edit-actions">
            <button type="button" class="btn-save" id="ct-submit">Create table</button>
            <a class="btn-cancel" href="/console/ui/tables">Cancel</a>
          </div>
          <p class="err-line hidden" id="ct-err"></p>
        </form>
      </div>`;
    wireCreateTableForm();
  }

  function wireCreateTableForm() {
    const form = document.getElementById("create-table-form");
    wireSegmented(form);
    wireToggles(form);

    // Sort key on/off gates the sort-key fields and the LSI section's own
    // "way out" — see this section's header comment for why the toggle
    // itself defaults on rather than the section defaulting blocked.
    const skToggle = form.querySelector('[data-field="ct-sk-enabled"]');
    const skFields = document.getElementById("ct-sk-fields");
    const addLsiBtn = document.getElementById("ct-add-lsi");
    const lsiRows = document.getElementById("ct-lsi-rows");
    const lsiBlocked = document.getElementById("ct-lsi-blocked");
    function syncSortKeyUi() {
      const on = skToggle.classList.contains("on");
      skFields.classList.toggle("hidden", !on);
      addLsiBtn.disabled = !on;
      lsiRows.classList.toggle("hidden", !on);
      lsiBlocked.classList.toggle("hidden", on);
    }
    skToggle.addEventListener("click", syncSortKeyUi);
    syncSortKeyUi();

    addLsiBtn.addEventListener("click", () => {
      lsiRows.insertAdjacentHTML("beforeend", lsiFormRowHtml());
    });
    lsiRows.addEventListener("click", (e) => {
      if (e.target.hasAttribute("data-remove-row")) {
        e.target.closest("[data-lsi-row]").remove();
      }
    });

    const gsiRows = document.getElementById("ct-gsi-rows");
    document.getElementById("ct-add-gsi").addEventListener("click", () => {
      gsiRows.insertAdjacentHTML("beforeend", gsiFormRowHtml());
      wireGsiRow(gsiRows.lastElementChild);
    });
    gsiRows.addEventListener("click", (e) => {
      if (e.target.hasAttribute("data-remove-row")) {
        e.target.closest("[data-gsi-row]").remove();
      }
    });

    const streamToggle = form.querySelector('[data-field="ct-stream-enabled"]');
    const streamViewField = document.getElementById("ct-stream-view-field");
    streamToggle.addEventListener("click", () => {
      streamViewField.classList.toggle("hidden", !streamToggle.classList.contains("on"));
    });

    const ttlToggle = form.querySelector('[data-field="ct-ttl-enabled"]');
    const ttlAttrField = document.getElementById("ct-ttl-attr-field");
    ttlToggle.addEventListener("click", () => {
      ttlAttrField.classList.toggle("hidden", !ttlToggle.classList.contains("on"));
    });

    document.getElementById("ct-submit").addEventListener("click", submitCreateTable);
  }

  async function submitCreateTable() {
    const errEl = document.getElementById("ct-err");
    const fail = (msg) => {
      errEl.textContent = msg;
      errEl.classList.remove("hidden");
    };
    errEl.classList.add("hidden");

    const tableName = document.getElementById("ct-name").value.trim();
    const pkName = document.getElementById("ct-pk-name").value.trim();
    if (!tableName || !pkName) {
      fail("Table name and partition key attribute name are both required.");
      return;
    }
    const req = {
      table_name: tableName,
      partition_key: { name: pkName, attribute_type: document.getElementById("ct-pk-type").value },
    };

    const skOn = toggleValue(document.getElementById("create-table-form"), "ct-sk-enabled");
    if (skOn) {
      const skName = document.getElementById("ct-sk-name").value.trim();
      if (!skName) {
        fail("Sort key attribute name is required when the sort key is enabled.");
        return;
      }
      req.sort_key = { name: skName, attribute_type: document.getElementById("ct-sk-type").value };
    }

    const lsis = [];
    for (const row of document.querySelectorAll("#ct-lsi-rows [data-lsi-row]")) {
      const indexName = row.querySelector("[data-lsi-name]").value.trim();
      const sortAttribute = row.querySelector("[data-lsi-sort]").value.trim();
      if (!indexName && !sortAttribute) continue; // an untouched blank row
      if (!indexName || !sortAttribute) {
        fail("Every LSI needs both an index name and a sort key attribute.");
        return;
      }
      lsis.push({ index_name: indexName, sort_attribute: sortAttribute });
    }
    req.lsis = lsis;

    const gsis = [];
    for (const row of document.querySelectorAll("#ct-gsi-rows [data-gsi-row]")) {
      const indexName = row.querySelector("[data-gsi-name]").value.trim();
      const hashAttribute = row.querySelector("[data-gsi-hash]").value.trim();
      const sortAttribute = row.querySelector("[data-gsi-sort]").value.trim();
      const projectionType = segmentedValue(row, "gsi-projection-type") || "ALL";
      if (!indexName && !hashAttribute) continue; // an untouched blank row
      if (!indexName || !hashAttribute) {
        fail("Every GSI needs both an index name and a hash attribute.");
        return;
      }
      const gsi = { index_name: indexName, hash_attribute: hashAttribute, projection_type: projectionType };
      if (sortAttribute) gsi.sort_attribute = sortAttribute;
      if (projectionType === "INCLUDE") {
        const names = row
          .querySelector("[data-gsi-nonkey]")
          .value.split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        if (names.length === 0) {
          fail(`GSI “${indexName}”'s INCLUDE projection needs at least one non-key attribute.`);
          return;
        }
        gsi.projection_non_key_attributes = names;
      }
      gsis.push(gsi);
    }
    req.gsis = gsis;

    req.stream_enabled = toggleValue(document.getElementById("create-table-form"), "ct-stream-enabled");
    if (req.stream_enabled) {
      req.stream_view_type = segmentedValue(document.getElementById("create-table-form"), "ct-stream-view-type") || STREAM_VIEW_TYPES[0];
    }

    req.ttl_enabled = toggleValue(document.getElementById("create-table-form"), "ct-ttl-enabled");
    if (req.ttl_enabled) {
      const ttlAttr = document.getElementById("ct-ttl-attr").value.trim();
      if (!ttlAttr) {
        fail("TTL attribute name is required when TTL is enabled.");
        return;
      }
      req.ttl_attribute_name = ttlAttr;
    }

    const submitBtn = document.getElementById("ct-submit");
    submitBtn.disabled = true;
    try {
      const resp = await postJSON("/console/api/tables", req);
      window.location.href = tableHref(resp.table.name);
    } catch (e) {
      fail(String(e.message || e));
      submitBtn.disabled = false;
    }
  }

  // ---- the table page: Config tab ------------------------------------------
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
  // DynamoDB's own `AttributeType` — a genuinely closed set (issue #319:
  // the Add-GSI form's type picker below).
  const ATTRIBUTE_TYPES = ["S", "N", "B"];

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
  // stream view type, an attribute type) — never for a free-text attribute
  // name (see the module doc / `console.rs`'s own doc on why a picker would
  // misrepresent the data model there).
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

  // The table page has three tabs — Config (PR3; default), Items (PR4), and
  // Stream data (this PR). Each is its own real URL
  // (`tableHref`/`tableItemsHref`/`tableStreamHref`) rather than one shared
  // page toggling sections via `dashboard_core.js::activateTab`'s pushState
  // idiom: unlike that dashboard's tabs (which all share one already-fetched
  // status blob) or this same page's own Settings/Indexes/Danger-zone jump
  // nav (which all share one `table_detail` fetch and just scroll within
  // it), Config/Items/Stream data are genuinely different data-fetching
  // screens — each makes its own calls the others never touch. A real
  // navigation keeps that boundary explicit, costs nothing extra (every
  // route serves the identical static shell, `console::is_shell_path`), and
  // makes "Config is the default tab" a structural fact of the URL space
  // (arriving at the bare `tableHref` always renders Config) rather than
  // client state that could start on the wrong tab.
  async function renderTablePage(name, tab) {
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
        <nav class="tab-strip">
          <a class="tab-link${tab === "config" ? " active" : ""}" href="${tableHref(name)}">Config</a>
          <a class="tab-link${tab === "items" ? " active" : ""}" href="${tableItemsHref(name)}">Items</a>
          <a class="tab-link${tab === "stream" ? " active" : ""}" href="${tableStreamHref(name)}">Stream data</a>
        </nav>
        <div id="table-tab-content"></div>
      </div>`;
    const content = document.getElementById("table-tab-content");
    if (tab === "items") {
      renderItemsTab(content);
    } else if (tab === "stream") {
      renderStreamTab(content);
    } else {
      renderConfigTab(content);
    }
  }

  // -- Config tab: Settings / Indexes / Danger zone under one jump nav -----

  function renderConfigTab(root) {
    root.innerHTML = `
      <nav class="jump-nav">
        <a href="#settings">Settings</a>
        <a href="#indexes">Indexes</a>
        <a href="#danger">Danger zone</a>
      </nav>
      <section id="settings" class="config-section"></section>
      <section id="indexes" class="config-section"></section>
      <section id="danger" class="config-section"></section>`;
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
          <div class="field">Hash attribute type (optional)${segmented("gsi-hash-attr-type", ATTRIBUTE_TYPES, null)}</div>
          <label class="field">Sort attribute (optional)
            <input type="text" class="attr-input" id="gsi-sort-attr" placeholder="attribute name" autocomplete="off">
          </label>
          <div class="field">Sort attribute type (optional)${segmented("gsi-sort-attr-type", ATTRIBUTE_TYPES, null)}</div>
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
    // Issue #319: the type pickers are optional — an unselected one sends no
    // type, and the index reads back untyped exactly as it always did.
    const req = { index_name: indexName, hash_attribute: hashAttribute };
    const hashAttributeType = segmentedValue(scope, "gsi-hash-attr-type");
    if (hashAttributeType) {
      req.hash_attribute_type = hashAttributeType;
    }
    if (sortAttribute) {
      req.sort_attribute = sortAttribute;
      const sortAttributeType = segmentedValue(scope, "gsi-sort-attr-type");
      if (sortAttributeType) {
        req.sort_attribute_type = sortAttributeType;
      }
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

  // ---- the table page: Items tab (this PR) --------------------------------
  //
  // Browse (Scan, paginated by DynamoDB's own ExclusiveStartKey/
  // LastEvaluatedKey — "Load more" carries the last page's cursor forward,
  // never a fake offset), Query (by partition key, plus a sort-key
  // condition when the target has one), or Get (one item by its exact key)
  // a table's own rows, or one of its declared GSIs'/LSIs' for Scan/Query
  // (a real closed set — TABLE_DETAIL's own gsis/lsis — rendered with a
  // <select>, never free text; Get is always base-table-only, matching
  // real DynamoDB's own GetItem, which takes no IndexName). Every item
  // round-trips in DynamoDB's own wire shape (`console::WireItem`) — see
  // `console.rs`'s module doc for why that's deliberate, not a shortcut:
  // the item editor below is what turns that shape into something a person
  // can read and edit without ever fabricating a type the server didn't
  // actually record.
  //
  // A key attribute (partition or sort, base table or index) is always
  // scalar in DynamoDB (S/N/B) — so its *value* input offers a real S/N/B
  // control (`keyValueInputHtml`), same as `console.rs`'s own doc reasons
  // for `SortKeyQuery`. That is different from an ordinary item attribute's
  // *type*, which is asked for with a plain `<select>` over the full closed
  // wire-type set (`ATTR_TYPES`) precisely because — unlike a key — any of
  // the eight actually applies. Neither is a free-text guess at a value that
  // could be anything: an attribute *name* is the one thing in this whole UI
  // that stays `<input type="text">`, because it's the one thing that
  // genuinely is free text (any per-item attribute, undeclared anywhere in
  // the catalog).

  let ITEMS_STATE = null;

  function itemsApiPath(tail) {
    return tableApiPath(TABLE_DETAIL.name, `items/${tail}`);
  }

  // Item CRUD always targets the *base* table — DynamoDB has no direct
  // write to a GSI/LSI, both are views materialized from base writes — so
  // the key attributes an item is identified by are always the base table's
  // own, regardless of which source (base/GSI/LSI) a row was scanned or
  // queried from. AWS always includes the base table's key attributes in an
  // index's own projection (needed to fetch the full item), which is the
  // property this relies on.
  function baseKeyNames() {
    return {
      pk: TABLE_DETAIL.partition_key.name,
      sk: TABLE_DETAIL.sort_key ? TABLE_DETAIL.sort_key.name : null,
    };
  }

  // The partition/sort attribute names to scan/query by for the currently
  // selected source (`""` = base table, `"gsi:name"`/`"lsi:name"` = one of
  // this table's own declared indexes).
  function sourceKeyNames(sourceValue) {
    if (!sourceValue) return baseKeyNames();
    const [kind, name] = sourceValue.split(":");
    const list = kind === "gsi" ? TABLE_DETAIL.gsis : TABLE_DETAIL.lsis;
    const found = (list || []).find((x) => x.name === name);
    if (!found) return baseKeyNames();
    if (kind === "gsi") {
      return {
        pk: found.hash_attribute.name,
        sk: found.sort_attribute ? found.sort_attribute.name : null,
      };
    }
    // An LSI shares the base table's own partition key and adds its own
    // alternate sort key.
    return { pk: TABLE_DETAIL.partition_key.name, sk: found.sort_attribute.name };
  }

  function sourceOptionsHtml() {
    const gsiOpts = (TABLE_DETAIL.gsis || [])
      .map((g) => `<option value="gsi:${esc(g.name)}">GSI: ${esc(g.name)}</option>`)
      .join("");
    const lsiOpts = (TABLE_DETAIL.lsis || [])
      .map((l) => `<option value="lsi:${esc(l.name)}">LSI: ${esc(l.name)}</option>`)
      .join("");
    return `<option value="">Base table</option>${gsiOpts}${lsiOpts}`;
  }

  // A key attribute's *value* input: a real S/N/B control (a key is always
  // scalar in DynamoDB) plus a text box for the value itself. Returns the
  // exact `{"S": "..."}`/`{"N": "..."}`/`{"B": "..."}` shape sent on the
  // wire — never a guessed/defaulted type.
  function keyValueInputHtml(cls) {
    return `<span class="key-value-input ${cls}">
      <select class="kv-type"><option value="S">S</option><option value="N">N</option><option value="B">B</option></select>
      <input type="text" class="attr-input kv-value" autocomplete="off">
    </span>`;
  }
  function readKeyValueInput(root, cls) {
    const scope = root.querySelector(`.key-value-input.${cls}`);
    const type = scope.querySelector(".kv-type").value;
    const raw = scope.querySelector(".kv-value").value;
    return { [type]: raw };
  }

  function renderItemsTab(root) {
    ITEMS_STATE = { items: [], lastKey: null };
    root.innerHTML = `
      <div class="items-tab">
        <div class="items-toolbar">
          <label class="field">Source
            <select id="items-source">${sourceOptionsHtml()}</select>
          </label>
          <div class="field">Mode${segmented("items-mode", ["Scan", "Query", "Get"], "Scan")}</div>
          <div id="items-query-fields"></div>
          <label class="field" id="items-limit-field">Limit
            <input type="text" id="items-limit" class="attr-input" style="width:80px" value="25" autocomplete="off">
          </label>
          <div class="items-actions">
            <button type="button" class="btn-save" id="items-run">Scan</button>
            <button type="button" class="btn-new" id="items-new">+ New item</button>
          </div>
        </div>
        <p class="err-line hidden" id="items-err"></p>
        <div class="items-card">
          <div class="table-scroll">
            <table class="items-table">
              <thead><tr id="items-head"></tr></thead>
              <tbody id="items-body"></tbody>
            </table>
          </div>
        </div>
        <div class="items-footer">
          <span class="muted" id="items-count"></span>
          <button type="button" class="btn-edit hidden" id="items-load-more">Load more</button>
        </div>
      </div>
      <div class="item-editor-overlay hidden" id="item-editor">
        <div class="item-editor-card">
          <h3 id="item-editor-title"></h3>
          <div class="attr-rows" id="item-editor-rows"></div>
          <button type="button" class="btn-edit" id="item-editor-add-attr">+ Add attribute</button>
          <div class="edit-actions">
            <button type="button" class="btn-save" id="item-editor-save">Save item</button>
            <button type="button" class="btn-cancel" id="item-editor-cancel">Cancel</button>
          </div>
          <p class="err-line hidden" id="item-editor-err"></p>
        </div>
      </div>`;

    wireSegmented(root);
    document.getElementById("items-source").addEventListener("change", renderItemsQueryFields);
    root.querySelectorAll('.segmented[data-field="items-mode"] .seg-opt').forEach((btn) => {
      btn.addEventListener("click", renderItemsQueryFields);
    });
    document.getElementById("items-run").addEventListener("click", () => runItemsQuery(false));
    document.getElementById("items-load-more").addEventListener("click", () => runItemsQuery(true));
    document.getElementById("items-new").addEventListener("click", () => openItemEditor(null));
    document.getElementById("item-editor-add-attr").addEventListener("click", addEditorRow);
    document.getElementById("item-editor-cancel").addEventListener("click", closeItemEditor);
    document.getElementById("item-editor-save").addEventListener("click", saveItemEditor);
    wireEditorRowsDelegation();

    renderItemsQueryFields();
    renderItemsHead();
    renderItemsRows();
    runItemsQuery(false);
  }

  function renderItemsQueryFields() {
    const el = document.getElementById("items-query-fields");
    const mode = segmentedValue(document, "items-mode") || "Scan";
    document.getElementById("items-run").textContent = mode;
    document.getElementById("items-limit-field").classList.toggle("hidden", mode !== "Scan");
    // GetItem is always a base-table operation in DynamoDB (no `IndexName`
    // parameter exists on it) — the Source select only makes sense for
    // Scan/Query, so disable it rather than silently ignoring whatever it's
    // set to.
    document.getElementById("items-source").disabled = mode === "Get";
    if (mode === "Scan") {
      el.innerHTML = "";
      return;
    }
    if (mode === "Get") {
      const keys = baseKeyNames();
      el.innerHTML = `
        <div class="field">Partition key (<span class="mono">${esc(keys.pk)}</span>)${keyValueInputHtml("items-pk")}</div>
        ${
          keys.sk
            ? `<div class="field">Sort key (<span class="mono">${esc(keys.sk)}</span>)${keyValueInputHtml("items-sort-value")}</div>`
            : ""
        }`;
      return;
    }
    const keys = sourceKeyNames(document.getElementById("items-source").value);
    el.innerHTML = `
      <div class="field">Partition key (<span class="mono">${esc(keys.pk)}</span>)${keyValueInputHtml("items-pk")}</div>
      ${
        keys.sk
          ? `<div class="field">Sort key (<span class="mono">${esc(keys.sk)}</span>)${segmented(
              "items-sort-op",
              ["any", "=", "between", "begins_with"],
              "any"
            )}</div>
             <div id="items-sort-values"></div>`
          : ""
      }`;
    wireSegmented(el);
    if (keys.sk) {
      el.querySelectorAll('.segmented[data-field="items-sort-op"] .seg-opt').forEach((btn) => {
        btn.addEventListener("click", renderItemsSortValueInputs);
      });
      renderItemsSortValueInputs();
    }
  }

  function renderItemsSortValueInputs() {
    const el = document.getElementById("items-sort-values");
    const op = segmentedValue(document, "items-sort-op") || "any";
    if (op === "any") {
      el.innerHTML = "";
    } else if (op === "between") {
      el.innerHTML = `<div class="field-row-2">
        <div class="field">From${keyValueInputHtml("items-sort-lo")}</div>
        <div class="field">To${keyValueInputHtml("items-sort-hi")}</div>
      </div>`;
    } else {
      el.innerHTML = `<div class="field">Value${keyValueInputHtml("items-sort-value")}</div>`;
    }
  }

  async function runItemsQuery(loadMore) {
    const errEl = document.getElementById("items-err");
    const runBtn = document.getElementById("items-run");
    errEl.classList.add("hidden");
    runBtn.disabled = true;
    const mode = segmentedValue(document, "items-mode") || "Scan";
    const sourceValue = document.getElementById("items-source").value;
    const indexName = sourceValue ? sourceValue.split(":").slice(1).join(":") : undefined;
    try {
      let page;
      if (mode === "Get") {
        const keys = baseKeyNames();
        const key = { [keys.pk]: readKeyValueInput(document, "items-pk") };
        if (keys.sk) key[keys.sk] = readKeyValueInput(document, "items-sort-value");
        const resp = await postJSON(itemsApiPath("get"), { key });
        page = { items: resp.item ? [resp.item] : [], last_evaluated_key: null };
        loadMore = false;
      } else if (mode === "Scan") {
        const req = {};
        if (indexName) req.index_name = indexName;
        const limitRaw = document.getElementById("items-limit").value.trim();
        if (limitRaw) req.limit = parseInt(limitRaw, 10);
        if (loadMore && ITEMS_STATE.lastKey) req.exclusive_start_key = ITEMS_STATE.lastKey;
        page = await postJSON(itemsApiPath("scan"), req);
      } else {
        const req = { partition_value: readKeyValueInput(document, "items-pk") };
        if (indexName) req.index_name = indexName;
        const op = segmentedValue(document, "items-sort-op") || "any";
        if (op === "=") {
          req.sort_condition = { kind: "equals", value: readKeyValueInput(document, "items-sort-value") };
        } else if (op === "between") {
          req.sort_condition = {
            kind: "between",
            lo: readKeyValueInput(document, "items-sort-lo"),
            hi: readKeyValueInput(document, "items-sort-hi"),
          };
        } else if (op === "begins_with") {
          req.sort_condition = {
            kind: "begins_with",
            value: readKeyValueInput(document, "items-sort-value"),
          };
        }
        page = await postJSON(itemsApiPath("query"), req);
        loadMore = false; // Query has no pagination on this adapter (see console.rs's ItemsPage doc)
      }
      ITEMS_STATE.items = loadMore ? ITEMS_STATE.items.concat(page.items) : page.items;
      ITEMS_STATE.lastKey = page.last_evaluated_key || null;
      renderItemsRows();
      document.getElementById("items-load-more").classList.toggle("hidden", !ITEMS_STATE.lastKey);
      const n = ITEMS_STATE.items.length;
      document.getElementById("items-count").textContent = `${n} item${n === 1 ? "" : "s"} loaded`;
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
    } finally {
      runBtn.disabled = false;
    }
  }

  function renderItemsHead() {
    const keys = baseKeyNames();
    document.getElementById("items-head").innerHTML = `
      <th>${esc(keys.pk)}</th>
      ${keys.sk ? `<th>${esc(keys.sk)}</th>` : ""}
      <th>Attributes</th>
      <th></th>`;
  }

  // A compact, honest rendering of one `AttributeValue` — every branch
  // renders the tag that's actually there, never a fabricated one; an
  // unrecognized shape (a decode failure server-side could never produce,
  // but a hand-edited raw attribute in-flight to being saved could) renders
  // as a literal dash rather than guessing.
  function renderAttributeValueCompact(av) {
    if (!av || typeof av !== "object") return '<span class="dash">—</span>';
    if ("S" in av) return esc(av.S);
    if ("N" in av) return esc(av.N);
    if ("BOOL" in av) return av.BOOL ? "true" : "false";
    if ("NULL" in av) return '<span class="muted">null</span>';
    if ("B" in av) return '<span class="muted">(binary)</span>';
    if ("SS" in av) return `<span class="muted">{${av.SS.length} strings}</span>`;
    if ("NS" in av) return `<span class="muted">{${av.NS.length} numbers}</span>`;
    if ("BS" in av) return `<span class="muted">{${av.BS.length} binaries}</span>`;
    if ("L" in av) return `<span class="muted">[${av.L.length} items]</span>`;
    if ("M" in av) return `<span class="muted">{${Object.keys(av.M).length} attrs}</span>`;
    return '<span class="dash">—</span>';
  }

  // A compact one-line preview of every non-key attribute on `item` — the
  // Items tab's answer to "items have no fixed column set" (see this
  // section's own header comment): rather than a union-of-attributes table
  // (sparse and ever-widening across a heterogeneous page) or an
  // always-expanded row, one preview column carries a glance-level summary
  // and the row's own Edit action is the full, honest view of every
  // attribute (the item editor below, pre-filled from this exact row's
  // already-loaded data — no extra `GetItem` round trip needed to edit what
  // a scan/query just returned).
  function attributePreview(item, keys) {
    const parts = Object.keys(item)
      .filter((name) => name !== keys.pk && name !== keys.sk)
      .sort()
      .map((name) => `${esc(name)}=${renderAttributeValueCompact(item[name])}`);
    if (parts.length === 0) return '<span class="dash">—</span>';
    return parts.join(", ");
  }

  // The Stream tab's sibling of `attributePreview` above: every attribute of
  // a raw attribute map (a stream record's `Keys`/`OldImage`/`NewImage`, none
  // of which have a "key attribute to skip" the way an item row does), same
  // honest-compact rendering (`renderAttributeValueCompact` — never a
  // fabricated type).
  function attributeMapPreview(map) {
    if (!map) return '<span class="dash">—</span>';
    const parts = Object.keys(map)
      .sort()
      .map((name) => `${esc(name)}=${renderAttributeValueCompact(map[name])}`);
    if (parts.length === 0) return '<span class="dash">—</span>';
    return parts.join(", ");
  }

  function itemKeyOf(item, keys) {
    const key = {};
    if (item[keys.pk] !== undefined) key[keys.pk] = item[keys.pk];
    if (keys.sk && item[keys.sk] !== undefined) key[keys.sk] = item[keys.sk];
    return key;
  }

  function renderItemsRows() {
    const body = document.getElementById("items-body");
    const keys = baseKeyNames();
    const cols = keys.sk ? 4 : 3;
    if (ITEMS_STATE.items.length === 0) {
      body.innerHTML = `<tr><td colspan="${cols}"><div class="empty-state">No items loaded. Run a scan or query above.</div></td></tr>`;
      return;
    }
    body.innerHTML = ITEMS_STATE.items
      .map(
        (item, i) => `
      <tr>
        <td class="mono">${renderAttributeValueCompact(item[keys.pk])}</td>
        ${keys.sk ? `<td class="mono">${renderAttributeValueCompact(item[keys.sk])}</td>` : ""}
        <td class="attrs-preview">${attributePreview(item, keys)}</td>
        <td class="row-actions">
          <button type="button" class="btn-edit" data-edit-item="${i}">Edit</button>
          <button type="button" class="btn-drop" data-delete-item="${i}">Delete</button>
        </td>
      </tr>`
      )
      .join("");
    body.querySelectorAll("[data-edit-item]").forEach((btn) => {
      btn.addEventListener("click", () => openItemEditor(ITEMS_STATE.items[Number(btn.dataset.editItem)]));
    });
    body.querySelectorAll("[data-delete-item]").forEach((btn) => {
      btn.addEventListener("click", () => deleteItemRow(Number(btn.dataset.deleteItem), btn));
    });
  }

  async function deleteItemRow(index, btn) {
    const keys = baseKeyNames();
    const key = itemKeyOf(ITEMS_STATE.items[index], keys);
    if (!window.confirm("Delete this item? This cannot be undone.")) return;
    btn.disabled = true;
    try {
      await postJSON(itemsApiPath("delete"), { key });
      ITEMS_STATE.items.splice(index, 1);
      renderItemsRows();
      const n = ITEMS_STATE.items.length;
      document.getElementById("items-count").textContent = `${n} item${n === 1 ? "" : "s"} loaded`;
    } catch (e) {
      btn.disabled = false;
      window.alert(`Couldn't delete item: ${e.message || e}`);
    }
  }

  // -- the item editor: create or edit one item's full attribute set -------
  //
  // Every DynamoDB wire type (S/N/B/BOOL/NULL/L/M/SS/NS/BS — a real closed
  // set) gets a `<select>`; an attribute *name* stays free text (the one
  // thing here that genuinely is). S/N/B/BOOL/NULL each get a purpose-built
  // editor; the four collection types (L/M/SS/NS/BS) fall back to a "Raw
  // (JSON)" textarea holding that one attribute's own `AttributeValue` JSON
  // verbatim — editing exactly the wire bytes rather than a partial
  // recursive editor this PR doesn't attempt, so the value shown is always
  // the value that would actually be sent, never a lossy stand-in for it.
  // Key attributes (partition/sort) always lead and are locked (name + type
  // fixed) whenever an existing item is being edited: letting a key value
  // change under "Edit" would silently `PutItem` a *second* item at the new
  // key rather than rename this one, leaving the original behind — a
  // dishonest edit affordance this form structurally can't offer. A brand
  // new item's key rows stay editable (there is no existing identity to
  // preserve yet).

  const ATTR_TYPES = ["S", "N", "B", "BOOL", "NULL", "RAW"];

  function decomposeAttributeValue(av) {
    if (av && typeof av === "object") {
      const keys = Object.keys(av);
      if (keys.length === 1 && ["S", "N", "B", "BOOL", "NULL"].includes(keys[0])) {
        const k = keys[0];
        if (k === "BOOL") return { type: "BOOL", rawValue: av.BOOL === true };
        if (k === "NULL") return { type: "NULL", rawValue: "" };
        return { type: k, rawValue: String(av[k]) };
      }
    }
    return { type: "RAW", rawValue: JSON.stringify(av === undefined ? null : av) };
  }

  // `locked` disables the value editor too, not just the name/type — a
  // locked row is always a key attribute being edited (see `attrRowHtml`'s
  // doc), and letting its *value* change while its name/type stay fixed
  // would be exactly the same dishonest-edit trap under a different name:
  // the save still goes through as a `PutItem` at this item's existing key,
  // so a changed key value would silently write a second, different item
  // rather than move this one.
  function attrValueEditorHtml(type, rawValue, locked) {
    if (type === "BOOL") {
      // A locked BOOL value renders as an inert `<span>`, not the real
      // `<button>` `toggleSwitch` produces — never expected in practice
      // (a DynamoDB key attribute is always scalar S/N/B, never BOOL), kept
      // only as defense-in-depth so a locked row can never be toggled by
      // construction, not merely by convention.
      const on = rawValue === true;
      return locked
        ? `<span class="toggle-switch${on ? " on" : ""}" aria-checked="${on}"><span class="knob"></span></span>`
        : toggleSwitch("attr-value", on);
    }
    if (type === "NULL") return '<span class="muted">— (null)</span>';
    if (type === "RAW") {
      return `<textarea class="attr-input attr-raw" rows="2" placeholder='e.g. {"L":[{"S":"a"}]} or {"M":{"x":{"N":"1"}}}'${locked ? " readonly" : ""}>${esc(rawValue)}</textarea>`;
    }
    return `<input type="text" class="attr-input attr-scalar" value="${esc(rawValue)}" autocomplete="off"${locked ? " readonly" : ""}>`;
  }

  // A locked row is always a key attribute (partition or sort) on an
  // existing item's edit form — name, type, AND value all fixed (see
  // `attrValueEditorHtml`'s doc for why the value must be locked too). A
  // brand-new item's key rows are never locked: there is no existing
  // identity yet to protect.
  function attrRowHtml(name, type, rawValue, locked) {
    const typeOpts = ATTR_TYPES.map(
      (t) => `<option value="${t}"${t === type ? " selected" : ""}>${t === "RAW" ? "Raw (JSON)" : t}</option>`
    ).join("");
    return `<div class="attr-row" data-locked="${locked ? "1" : "0"}">
      <input type="text" class="attr-input attr-name" placeholder="attribute name" value="${esc(name)}" autocomplete="off"${locked ? " disabled" : ""}>
      <select class="attr-type"${locked ? " disabled" : ""}>${typeOpts}</select>
      <span class="attr-value">${attrValueEditorHtml(type, rawValue, locked)}</span>
      ${locked ? '<span class="key-badge">key</span>' : '<button type="button" class="btn-drop" data-remove-row>Remove</button>'}
    </div>`;
  }

  // Event delegation on the (static) rows container — wired exactly once
  // per Items-tab render, so adding/removing rows never needs re-wiring
  // (and can never double-wire) the rows that already exist.
  function wireEditorRowsDelegation() {
    const rowsEl = document.getElementById("item-editor-rows");
    rowsEl.addEventListener("change", (e) => {
      if (!e.target.classList.contains("attr-type")) return;
      const row = e.target.closest(".attr-row");
      // A disabled <select> never fires `change`, so `row` is never locked
      // here — `locked: false` is simply what a freshly-typed-into row is.
      row.querySelector(".attr-value").innerHTML = attrValueEditorHtml(e.target.value, "", false);
    });
    rowsEl.addEventListener("click", (e) => {
      const toggle = e.target.closest(".toggle-switch");
      if (toggle) {
        if (toggle.closest(".attr-row")?.dataset.locked === "1") return;
        const on = !toggle.classList.contains("on");
        toggle.classList.toggle("on", on);
        toggle.setAttribute("aria-checked", String(on));
        return;
      }
      if (e.target.hasAttribute("data-remove-row")) {
        e.target.closest(".attr-row").remove();
      }
    });
  }

  function addEditorRow() {
    document
      .getElementById("item-editor-rows")
      .insertAdjacentHTML("beforeend", attrRowHtml("", "S", "", false));
  }

  function openItemEditor(item) {
    const keys = baseKeyNames();
    document.getElementById("item-editor-title").textContent = item ? "Edit item" : "New item";
    document.getElementById("item-editor-err").classList.add("hidden");
    const rowsHtml = [];
    const pk = item ? decomposeAttributeValue(item[keys.pk]) : { type: "S", rawValue: "" };
    rowsHtml.push(attrRowHtml(keys.pk, pk.type, pk.rawValue, !!item));
    if (keys.sk) {
      const sk = item ? decomposeAttributeValue(item[keys.sk]) : { type: "S", rawValue: "" };
      rowsHtml.push(attrRowHtml(keys.sk, sk.type, sk.rawValue, !!item));
    }
    if (item) {
      Object.keys(item)
        .filter((name) => name !== keys.pk && name !== keys.sk)
        .sort()
        .forEach((name) => {
          const d = decomposeAttributeValue(item[name]);
          rowsHtml.push(attrRowHtml(name, d.type, d.rawValue, false));
        });
    }
    document.getElementById("item-editor-rows").innerHTML = rowsHtml.join("");
    document.getElementById("item-editor").classList.remove("hidden");
  }

  function closeItemEditor() {
    document.getElementById("item-editor").classList.add("hidden");
  }

  // Reads every attribute row back into DynamoDB wire shape, or an error
  // message on the first row that doesn't parse. A locked (key) row's
  // inputs are all read-only/disabled (never editable — see
  // `attrValueEditorHtml`'s doc), but a read-only input's `.value` still
  // reads normally, so its current value round-trips into the saved item
  // unchanged rather than being dropped.
  function readEditorRows() {
    const item = {};
    let error = null;
    document.querySelectorAll("#item-editor-rows .attr-row").forEach((row) => {
      if (error) return;
      const name = row.querySelector(".attr-name").value.trim();
      const type = row.querySelector(".attr-type").value;
      if (!name) {
        error = "Every attribute needs a name.";
        return;
      }
      if (type === "BOOL") {
        const on = row.querySelector(".toggle-switch")?.classList.contains("on") || false;
        item[name] = { BOOL: on };
      } else if (type === "NULL") {
        item[name] = { NULL: true };
      } else if (type === "RAW") {
        const raw = row.querySelector(".attr-raw").value;
        try {
          item[name] = JSON.parse(raw);
        } catch (e) {
          error = `Attribute "${name}": invalid JSON (${e.message})`;
        }
      } else {
        item[name] = { [type]: row.querySelector(".attr-scalar").value };
      }
    });
    return { item, error };
  }

  async function saveItemEditor() {
    const errEl = document.getElementById("item-editor-err");
    errEl.classList.add("hidden");
    const { item, error } = readEditorRows();
    if (error) {
      errEl.textContent = error;
      errEl.classList.remove("hidden");
      return;
    }
    const saveBtn = document.getElementById("item-editor-save");
    saveBtn.disabled = true;
    try {
      await postJSON(itemsApiPath("put"), { item });
      closeItemEditor();
      // Refresh from the server rather than guessing the item's place in
      // the current page/sort order client-side.
      await runItemsQuery(false);
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
    } finally {
      saveBtn.disabled = false;
    }
  }

  // ---- the table page: Stream data tab (this PR) --------------------------
  //
  // A table's DynamoDB Streams shards and the records inside them, built on
  // the real `ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords`
  // wire operations (`console.rs`'s `stream_shards`/`get_shard_iterator`/
  // `get_stream_records`) — same "reuse the real wire path" rule as every
  // other mutating/reading endpoint in this app.
  //
  // A **shard id is deliberately rendered as-is**, unabbreviated and
  // copyable: it looks like `shardId-<tablet>-<epoch>`, but it is DynamoDB's
  // own public wire identifier — a real client receives exactly this string
  // from `DescribeStream` and passes it straight back to
  // `GetShardIterator`, so a developer debugging their own stream needs to
  // see and copy it, the same way they'd copy a partition key value. See
  // `console.rs`'s own module doc for the line this module draws between
  // "DynamoDB wire vocabulary" (a shard id, its `ParentShardId` lineage, a
  // sequence number — all shown here) and actual cluster shape (which
  // node/replica serves it — never shown, and never even reaches this
  // script, since `console.rs`'s response types have no such field to send).
  //
  // The **iterator type control is a real segmented control**, not a
  // free-text guess: DynamoDB's four (`TRIM_HORIZON`/`LATEST`/
  // `AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER`) are a genuinely closed
  // set, the same "closed set gets a real control" rule the Add-GSI/Query
  // forms already follow elsewhere in this app. A sequence number itself
  // (when `AT_`/`AFTER_` needs one) stays a plain text input — it's a value
  // a developer copies from an already-shown record's own `Sequence #`
  // column, not a closed set.
  //
  // **Paging a shard's records is the honest `NextShardIterator` walk**
  // (ADR 0042 §6), the record-viewer equivalent of the Items tab's
  // `ExclusiveStartKey` walk: "Load more records" always sends the
  // previous page's own returned iterator, never a fake offset, and a
  // `null` `NextShardIterator` renders as "shard drained" rather than
  // silently going quiet. The shard *list* itself paginates the same
  // honest way, over `DescribeStream`'s own real
  // `ExclusiveStartShardId`/`LastEvaluatedShardId` contract.
  //
  // **TTL-deleted records are called out** (ADR 0051 §7): a record whose
  // `userIdentity` is present (`{"PrincipalId": "dynamodb.amazonaws.com",
  // "Type": "Service"}`) was deleted by the TTL reaper, not a client
  // `DeleteItem` — exactly the fact a developer debugging "why did my row
  // vanish" needs, and exactly the field real DynamoDB Streams uses to
  // convey it, so it is rendered as a badge rather than left for the
  // developer to notice buried in a raw record dump.

  let STREAM_STATE = null;

  function streamApiPath(tail) {
    return tableApiPath(TABLE_DETAIL.name, `stream/${tail}`);
  }

  function renderStreamTab(root) {
    STREAM_STATE = {
      shards: [],
      lastShardId: null,
      viewType: null,
      selectedShardId: null,
      iterator: null,
      records: [],
    };
    root.innerHTML = `<div class="stream-tab"><div id="stream-body"><p class="loading">Loading stream…</p></div></div>`;
    loadStreamShards(false);
  }

  async function loadStreamShards(loadMore) {
    const bodyEl = document.getElementById("stream-body");
    try {
      const qs =
        loadMore && STREAM_STATE.lastShardId
          ? `?exclusive_start_shard_id=${encodeURIComponent(STREAM_STATE.lastShardId)}`
          : "";
      const page = await getJSON(streamApiPath("shards") + qs);
      if (!page.enabled) {
        renderStreamDisabled(bodyEl);
        return;
      }
      STREAM_STATE.shards = loadMore ? STREAM_STATE.shards.concat(page.shards) : page.shards;
      STREAM_STATE.lastShardId = page.last_evaluated_shard_id;
      STREAM_STATE.viewType = page.view_type;
      if (!loadMore) renderStreamEnabledShell(bodyEl);
      renderShardRows();
    } catch (e) {
      bodyEl.innerHTML = `<p class="err-line">Couldn't load the stream: ${esc(String(e.message || e))}</p>`;
    }
  }

  // The honest "no stream" answer — a plain message pointing at where to
  // turn one on, never an empty grid that looks broken (this PR's own
  // design brief; `console::StreamShardsPage`'s doc makes the same call
  // server-side: `enabled: false` is data, not an error).
  function renderStreamDisabled(el) {
    el.innerHTML = `
      <div class="empty-state stream-disabled">
        No stream enabled on this table.
        <div class="hint">Turn one on from <a href="${tableHref(TABLE_DETAIL.name)}#settings">Config → Settings</a>.</div>
      </div>`;
  }

  function renderStreamEnabledShell(el) {
    el.innerHTML = `
      <div class="fact-strip">
        <div class="fact"><span class="fact-label">View type</span><span class="fact-value view-type">${esc(STREAM_STATE.viewType)}</span></div>
      </div>
      <h2>Shards</h2>
      <div class="items-card">
        <div class="table-scroll"><table class="items-table">
          <thead><tr><th>Shard ID</th><th>Parent</th><th>Status</th><th></th></tr></thead>
          <tbody id="shards-body"></tbody>
        </table></div>
      </div>
      <div class="items-footer">
        <span class="muted" id="shards-count"></span>
        <button type="button" class="btn-edit hidden" id="shards-load-more">Load more shards</button>
      </div>
      <h2>Records</h2>
      <div id="records-panel" class="records-panel">
        <p class="muted">Select a shard above to read its records.</p>
      </div>`;
    document.getElementById("shards-load-more").addEventListener("click", () => loadStreamShards(true));
  }

  function shardStatusPill(open) {
    return open
      ? '<span class="status-pill shard-pill-open">Open</span>'
      : '<span class="status-pill shard-pill-closed">Closed</span>';
  }

  function shardRowHtml(s) {
    const open = s.ending_sequence_number == null;
    return `
      <tr>
        <td class="mono">${esc(s.shard_id)}</td>
        <td class="mono">${s.parent_shard_id ? esc(s.parent_shard_id) : '<span class="dash">—</span>'}</td>
        <td>${shardStatusPill(open)}</td>
        <td><button type="button" class="btn-edit" data-view-shard="${esc(s.shard_id)}">View records</button></td>
      </tr>`;
  }

  function renderShardRows() {
    const body = document.getElementById("shards-body");
    const count = document.getElementById("shards-count");
    if (!body) return;
    if (STREAM_STATE.shards.length === 0) {
      body.innerHTML = `<tr><td colspan="4"><div class="empty-state">No shards yet.</div></td></tr>`;
    } else {
      body.innerHTML = STREAM_STATE.shards.map(shardRowHtml).join("");
    }
    const n = STREAM_STATE.shards.length;
    count.textContent = `${n} shard${n === 1 ? "" : "s"} loaded`;
    document.getElementById("shards-load-more").classList.toggle("hidden", !STREAM_STATE.lastShardId);
    body.querySelectorAll("[data-view-shard]").forEach((btn) => {
      btn.addEventListener("click", () => selectShard(btn.dataset.viewShard));
    });
  }

  const ITERATOR_TYPES = ["TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"];

  function selectShard(shardId) {
    STREAM_STATE.selectedShardId = shardId;
    STREAM_STATE.iterator = null;
    STREAM_STATE.records = [];
    renderRecordsPanel();
  }

  function renderRecordsPanel() {
    const el = document.getElementById("records-panel");
    el.innerHTML = `
      <div class="stream-controls">
        <span class="mono shard-selected">${esc(STREAM_STATE.selectedShardId)}</span>
        <div class="field">Start position${segmented("stream-iter-type", ITERATOR_TYPES, "TRIM_HORIZON")}</div>
        <div class="field hidden" id="stream-seq-field">Sequence number
          <input type="text" id="stream-seq-value" class="attr-input" placeholder="from a record's Sequence #" autocomplete="off">
        </div>
        <button type="button" class="btn-save" id="stream-read-btn">Read records</button>
      </div>
      <p class="err-line hidden" id="stream-records-err"></p>
      <div class="items-card">
        <div class="table-scroll"><table class="items-table">
          <thead><tr><th>Event</th><th>Keys</th><th>Old image</th><th>New image</th><th>Sequence #</th><th>When</th></tr></thead>
          <tbody id="records-body"><tr><td colspan="6"><div class="empty-state">Choose a start position and read records.</div></td></tr></tbody>
        </table></div>
      </div>
      <div class="items-footer">
        <span class="muted" id="records-count"></span>
        <button type="button" class="btn-edit hidden" id="records-load-more">Load more records</button>
      </div>`;
    wireSegmented(el);
    el.querySelectorAll('.segmented[data-field="stream-iter-type"] .seg-opt').forEach((btn) => {
      btn.addEventListener("click", () => {
        const t = segmentedValue(el, "stream-iter-type");
        const needsSeq = t === "AT_SEQUENCE_NUMBER" || t === "AFTER_SEQUENCE_NUMBER";
        document.getElementById("stream-seq-field").classList.toggle("hidden", !needsSeq);
      });
    });
    document.getElementById("stream-read-btn").addEventListener("click", () => startReadingShard(el));
    document.getElementById("records-load-more").addEventListener("click", () => fetchRecords(true));
  }

  async function startReadingShard(scope) {
    const errEl = document.getElementById("stream-records-err");
    errEl.classList.add("hidden");
    const iterType = segmentedValue(scope, "stream-iter-type") || "TRIM_HORIZON";
    const seqInput = document.getElementById("stream-seq-value");
    const seq = seqInput ? seqInput.value.trim() : "";
    if ((iterType === "AT_SEQUENCE_NUMBER" || iterType === "AFTER_SEQUENCE_NUMBER") && !seq) {
      errEl.textContent = "A sequence number is required for this start position.";
      errEl.classList.remove("hidden");
      return;
    }
    const readBtn = document.getElementById("stream-read-btn");
    readBtn.disabled = true;
    try {
      const req = { shard_id: STREAM_STATE.selectedShardId, iterator_type: iterType };
      if (seq) req.sequence_number = seq;
      const resp = await postJSON(streamApiPath("iterator"), req);
      STREAM_STATE.iterator = resp.shard_iterator;
      STREAM_STATE.records = [];
      await fetchRecords(false);
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
    } finally {
      readBtn.disabled = false;
    }
  }

  async function fetchRecords(loadMore) {
    const errEl = document.getElementById("stream-records-err");
    errEl.classList.add("hidden");
    try {
      const resp = await postJSON(streamApiPath("records"), { shard_iterator: STREAM_STATE.iterator });
      STREAM_STATE.records = loadMore ? STREAM_STATE.records.concat(resp.records) : resp.records;
      // A `null` `next_shard_iterator` is DynamoDB's own "this shard is
      // exhausted" signal (ADR 0042 §2/§6) — rendered as "shard drained"
      // below, never silently treated as "try again later" the way a
      // still-open shard's temporary "nothing new yet" would be.
      STREAM_STATE.iterator = resp.next_shard_iterator;
      renderRecordRows();
      document.getElementById("records-load-more").classList.toggle("hidden", !STREAM_STATE.iterator);
      const n = STREAM_STATE.records.length;
      const drained = STREAM_STATE.iterator ? "" : " — shard drained";
      document.getElementById("records-count").textContent = `${n} record${n === 1 ? "" : "s"} loaded${drained}`;
    } catch (e) {
      errEl.textContent = String(e.message || e);
      errEl.classList.remove("hidden");
    }
  }

  function eventPill(name) {
    const cls = name === "INSERT" ? "pill-active" : name === "REMOVE" ? "pill-deleting" : "pill-creating";
    return `<span class="status-pill ${cls}">${esc(name || "?")}</span>`;
  }

  // The one place `userIdentity`'s presence turns into something a
  // developer actually notices (ADR 0051 §7) — see this section's own
  // header comment.
  function ttlBadge(record) {
    return record.userIdentity
      ? '<span class="status-pill shard-pill-ttl" title="Deleted by DynamoDB TTL expiry, not a client DeleteItem">TTL expiry</span>'
      : "";
  }

  function recordRowHtml(r) {
    const dd = r.dynamodb || {};
    const when =
      typeof dd.ApproximateCreationDateTime === "number"
        ? new Date(dd.ApproximateCreationDateTime * 1000).toLocaleString()
        : '<span class="dash">—</span>';
    return `
      <tr>
        <td>${eventPill(r.eventName)} ${ttlBadge(r)}</td>
        <td class="attrs-preview">${attributeMapPreview(dd.Keys)}</td>
        <td class="attrs-preview">${attributeMapPreview(dd.OldImage)}</td>
        <td class="attrs-preview">${attributeMapPreview(dd.NewImage)}</td>
        <td class="mono">${esc(dd.SequenceNumber || "")}</td>
        <td>${when}</td>
      </tr>`;
  }

  function renderRecordRows() {
    const body = document.getElementById("records-body");
    if (!body) return;
    if (STREAM_STATE.records.length === 0) {
      body.innerHTML = `<tr><td colspan="6"><div class="empty-state">No records in this range.</div></td></tr>`;
      return;
    }
    body.innerHTML = STREAM_STATE.records.map(recordRowHtml).join("");
  }

  // ---- routing -------------------------------------------------------------

  const path = normalizePath(window.location.pathname);
  if (path.startsWith(TABLES_ROUTE_PREFIX)) {
    const rest = path.slice(TABLES_ROUTE_PREFIX.length);
    if (rest === "new") {
      renderCreateTableForm();
    } else {
      // `{name}` (Config, the default), `{name}/items` (Items), or
      // `{name}/stream` (Stream data) — the only three shapes a real table
      // page's own URL takes; see `renderTablePage`'s own comment for why
      // this is a real route rather than a same-page tab toggle.
      const slash = rest.indexOf("/");
      const name = decodeURIComponent(slash === -1 ? rest : rest.slice(0, slash));
      const tail = slash === -1 ? "" : rest.slice(slash + 1);
      const tab = tail === "items" ? "items" : tail === "stream" ? "stream" : "config";
      renderTablePage(name, tab);
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
