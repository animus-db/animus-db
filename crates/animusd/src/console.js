// AnimusDB Data Console — client-side app (ADR 0052). Vanilla JS, no
// bundler, no dependencies: this file and `console.html`/`console.css` are
// the whole client. Every screen is a pure client of `GET /console/api/tables`
// (this listener's one JSON endpoint so far) — nothing here ever renders a
// node id, a tablet, a replica, or anything else cluster-shaped, mirroring
// the server-side rule `console.rs`'s own module doc states.
//
// Routing mirrors the operator dashboard's own idiom
// (`dashboard_core.js::activateTab`): the server always returns the same
// static shell for every `/console/ui/*` path (`console::is_shell_path`), and
// this script reads `location.pathname` once on load to decide what to
// render into `#app`. A real `<a href>` is used for every navigation (the
// tables list ↔ a table's own page ↔ the create-table form) rather than a
// client-side push/pop-state router — there is only ever one real screen so
// far, so a full request is simplest and, since the server serves the exact
// same shell either way, indistinguishable to the user from a client-side
// transition.
(function () {
  "use strict";

  const app = document.getElementById("app");

  const esc = (s) =>
    String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  async function getJSON(path) {
    const res = await fetch(path, { headers: { Accept: "application/json" } });
    if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
    return res.json();
  }

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

  // ---- a not-yet-built screen (a table's own page, the create-table form) -

  function renderStub(pathTail) {
    const isNew = pathTail === "new";
    const title = isNew ? "Create table" : pathTail;
    const body = isNew
      ? "The create-table form is not built yet."
      : "This table's own page is not built yet.";
    app.innerHTML = `
      <div class="stub">
        <h1>${esc(title)}</h1>
        <p>${body}</p>
        <a class="back" href="/console/ui/tables">← Back to tables</a>
      </div>`;
  }

  // ---- routing -------------------------------------------------------------

  const path = normalizePath(window.location.pathname);
  if (path.startsWith(TABLES_ROUTE_PREFIX)) {
    renderStub(decodeURIComponent(path.slice(TABLES_ROUTE_PREFIX.length)));
  } else if (TABLE_LIST_ROUTES.has(path)) {
    renderTablesList();
  } else {
    // Any other `/console/ui/*` deep link the shell serves but this router
    // doesn't yet recognize — the tables list is the console's landing
    // screen, so fall back to it rather than a bare blank page.
    renderTablesList();
  }
})();
