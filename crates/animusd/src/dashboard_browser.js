"use strict";
// The Data Browser view: a DynamoDB panel (table select, Scan/Query, an item
// list with per-row Edit/Delete, a detail panel, Create table / Create item
// forms, and the bulk-seed tool — which writes real DynamoDB items, hence
// lives here rather than in Storage) — all against the real
// /admin/data/{dynamo,drop-table,seed} endpoints, not
// the design mockup's fake in-memory item store. Depends on
// `dashboard_core.js` (STATE, $, esc, pill, getJSON, postJSON, loadAll,
// splitHiddenTable) and `dashboard_streams.js` (viewTypeLabel — the Stream
// row's enable/disable UI below lives here per ADR 0021, since a table's
// stream toggle is a table-panel action, not a Streams-tab one; also
// `tabletsForTable`, reused below for the Indexes card's backfill-progress
// count — a stream shard count and an index backfill count both need "every
// tablet currently mapped to this table", the same live-topology lookup).

// ---- DynamoDB: schema helpers (shared by Scan/Query/item forms) ----
// Excludes a GSI's own hidden `<base>$<index>` materialization table
// (`splitHiddenTable`, dashboard_core.js) — it's an implementation detail,
// not a table a user picks to browse/seed directly; reads against it go
// through the base table's Index selector instead (below).
function dynamoTables() {
  const t = STATE.status && STATE.status.schemas && STATE.status.schemas.tables;
  if (!t) return {};
  const out = {};
  for (const k of Object.keys(t)) if (!k.includes(".") && !splitHiddenTable(k)) out[k] = t[k];
  return out;
}
// "GSI" / "LSI" — the ADR 0041 `IndexKind` label as-serialized ("Global"/
// "Local", verified against `animus-control::schema::IndexKind`'s plain
// serde derive — no `rename_all`, so the enum tag rides as-is).
function indexKindLabel(kind) { return kind === "Global" ? "GSI" : "LSI"; }
// The ADR 0041 `IndexProjection`'s three shapes, as serialized: unit
// variants ride as their bare tag string; `Include` rides as `{Include:
// [...]}` — mirrors `IndexProjection` in animus-control::schema exactly.
function projectionLabel(p) {
  if (p === "All") return "ALL";
  if (p === "KeysOnly") return "KEYS_ONLY";
  if (p && p.Include) return `INCLUDE (${p.Include.length})`;
  return "—";
}
function dynColType(schema, name) {
  const col = (schema.columns || []).find((c) => c.name === name);
  const ty = col && col.ty;
  return ty === "Number" ? "N" : (ty === "Binary" ? "B" : "S");
}
function dynKeyNames(schema) {
  const names = [];
  if (schema.partition_key) names.push(schema.partition_key);
  const sk = schema.clustering_keys && schema.clustering_keys[0];
  if (sk) names.push(sk);
  return names;
}
function attrValue(a) {
  if (a.type === "N") return { N: a.value };
  if (a.type === "B") return { B: a.value };
  if (a.type === "BOOL") return { BOOL: a.value === "true" || a.value === "1" };
  return { S: a.value };
}
// Decode a raw AttributeValue to a display string, and recover its type tag —
// items are schemaless beyond the declared keys, so a scanned item's own
// AttributeValue variant is the only source of truth for non-key attributes.
function avDecode(v) {
  if (v == null) return "";
  if (v.N != null) return v.N;
  if (v.B != null) return v.B;
  if (v.BOOL != null) return String(v.BOOL);
  if (v.S != null) return v.S;
  return JSON.stringify(v);
}
function avType(v) {
  if (v == null) return "S";
  if (v.N != null) return "N";
  if (v.B != null) return "B";
  if (v.BOOL != null) return "BOOL";
  return "S";
}

// The op panel's own table selection. `lastRenderedDyTable` (read by
// dashboard_core.js's `render()`) tracks the value last built for, so a
// routine refresh with the same selection doesn't clobber in-progress edits.
let dyTable = "";
let lastRenderedDyTable;
let dyOp = "scan"; // 'scan' | 'query'
let dyPkValue = "";
let dySkOp = "=";
let dySkValue = "";
let dyFilterAttr = "";
let dyFilterValue = "";
// The Scan/Query Index selector's own state ("" = base table). Distinct
// from `dySelectedIndex` below (a *result row* index, unrelated concept —
// unfortunate name collision from the pre-existing code, kept as-is here).
let dyIndexName = "";
let dySelectedIndex = null;
let dyResultItems = []; // raw AttributeValue-map items from the last successful Scan/Query
let dyResultSummary = "";
let dyResultError = null;
let dyTableFormOpen = false;
let dyItemFormOpen = false;
let dyItemFormMode = "create";
let dyIxFormOpen = false; // the "Add index (GSI)" form, ADR 0045 §7

function renderBrowserTables() {
  const tables = dynamoTables();
  const names = Object.keys(tables).sort();
  if (!names.includes(dyTable)) dyTable = names[0] || "";
  const sel = $("br-dy-table");
  sel.innerHTML = names.length
    ? names.map((n) => `<option${n === dyTable ? " selected" : ""}>${esc(n)}</option>`).join("")
    : `<option value="">(none)</option>`;
  sel.value = dyTable;
  const validTable = !!dyTable;
  $("br-dy-no-tables").style.display = validTable ? "none" : "";
  $("br-dy-no-tables").textContent = names.length ? "" : "create a table first";
  ["br-dy-table", "br-dy-drop-table"].forEach((id) => { $(id).disabled = !validTable; });
  renderDynamoFields();
}

// The `#br-dy-table` selector's own change handler — updates immediately
// (not waiting for the next poll refresh) and marks the change as already
// handled so `render()`'s `dyTable !== lastRenderedDyTable` check doesn't
// redundantly re-run `dynOnTable` a second time.
function onDyTableChange() {
  dyTable = $("br-dy-table").value;
  lastRenderedDyTable = dyTable;
  dynOnTable();
}

// Switching table resets the op state and immediately browses the new table
// (Scan with no filter) — matching "picking a table shows you its data".
function dynOnTable() {
  dyOp = "scan"; dyPkValue = ""; dySkValue = ""; dyFilterAttr = ""; dyFilterValue = "";
  dyIndexName = ""; // a previously selected table's index name can't carry over
  dySelectedIndex = null; dyResultItems = []; dyResultError = null;
  closeItemForm();
  closeAddIndexForm();
  renderDynamoFields();
  setDynamoOp("scan");
  if (dyTable) runDynamoOp(); else renderDynamoResults();
}

// The currently-selected index's own `IndexDef` (ADR 0041), or `null` for
// the base table. Attribute *types* are always read off the base table's
// typed columns (`dynColType`, below) — an index has no typed columns of
// its own, only key attribute *names*.
function activeIndexDef(schema) {
  return (schema && dyIndexName) ? (schema.indexes || []).find((i) => i.name === dyIndexName) : null;
}
// Which attributes a Scan/Query keys on right now: the base table's own
// partition/sort key, or — with an index selected — that index's own hash/
// sort attribute.
function activeKeyNames(schema) {
  const idx = activeIndexDef(schema);
  if (!idx) return dynKeyNames(schema);
  return idx.sort_attribute ? [idx.hash_attribute, idx.sort_attribute] : [idx.hash_attribute];
}

function renderDynamoFields() {
  const schema = dynamoTables()[dyTable];
  const idx = activeIndexDef(schema);
  const [hashName, sortName] = schema ? activeKeyNames(schema) : [];
  $("br-dy-fields").innerHTML = !schema ? "" : `
    <div class="field"><div class="k">${idx ? "Index hash key" : "Partition key"}</div><div class="v">${esc(hashName || "—")} <span class="muted">(${esc(dynColType(schema, hashName))})</span></div></div>
    ${sortName
      ? `<div class="field"><div class="k">${idx ? "Index sort key" : "Sort key"}</div><div class="v">${esc(sortName)} <span class="muted">(${esc(dynColType(schema, sortName))})</span></div></div>`
      : ""}`;
  $("br-dy-query-sk-group").style.display = sortName ? "flex" : "none";
  renderIndexSelector(schema);
  renderIndexesSection(schema);
  renderStreamRow(schema);
}

// ---- DynamoDB Streams: enable/disable (ADR 0042/0043) ----
// A table's stream toggle lives here, in the Data Browser's own table panel,
// not the Streams tab (`dashboard_streams.js`) — the Streams tab is a
// cluster-wide *observability* view over every stream; enabling/disabling
// one is a per-table action, the same reasoning that already puts
// create/drop table here rather than there.
const STREAM_VIEW_TYPES = ["NEW_AND_OLD_IMAGES", "NEW_IMAGE", "OLD_IMAGE", "KEYS_ONLY"];

function renderStreamRow(schema) {
  const el = $("br-dy-stream");
  if (!schema) { el.innerHTML = ""; return; }
  const stream = schema.stream;
  if (stream) {
    el.innerHTML = `<div class="card scroll" style="margin-top:2px">
      <h2>Stream</h2>
      <div class="row" style="justify-content:space-between">
        <div class="row">${pill("ok", "ENABLED")}<span class="mono">${esc(viewTypeLabel(stream.view_type))}</span></div>
        <div class="row"><span class="muted" id="br-dy-stream-msg"></span><button class="danger-text" id="br-dy-stream-disable">Disable</button></div>
      </div>
    </div>`;
    $("br-dy-stream-disable").addEventListener("click", disableStream);
  } else {
    el.innerHTML = `<div class="card scroll" style="margin-top:2px">
      <h2>Stream</h2>
      <div class="row" style="justify-content:space-between">
        <span class="muted">no stream enabled</span>
        <div class="row">
          <span class="muted" id="br-dy-stream-msg"></span>
          <select id="br-dy-stream-vt">${STREAM_VIEW_TYPES.map((v) => `<option value="${v}">${v}</option>`).join("")}</select>
          <button id="br-dy-stream-enable">Enable</button>
        </div>
      </div>
    </div>`;
    $("br-dy-stream-enable").addEventListener("click", enableStream);
  }
}

async function enableStream() {
  const table = dyTable;
  const viewType = $("br-dy-stream-vt").value;
  if (!window.confirm(`Enable a DynamoDB Stream on “${table}” (view type ${viewType})? Only writes made after this point will appear in it.`)) return;
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "UpdateTable",
    payload: { TableName: table, StreamSpecification: { StreamEnabled: true, StreamViewType: viewType } },
  });
  if (status >= 300) {
    const msg = $("br-dy-stream-msg");
    if (msg) msg.innerHTML = `<span class="err-line">${esc((body && body.message) || `HTTP ${status}`)}</span>`;
    return;
  }
  await loadAll();
}

async function disableStream() {
  const table = dyTable;
  if (!window.confirm(`Disable “${table}”'s stream? It stays listed and readable until its retention window expires (F12-b's grace window) — this does not delete it immediately.`)) return;
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "UpdateTable",
    payload: { TableName: table, StreamSpecification: { StreamEnabled: false } },
  });
  if (status >= 300) {
    const msg = $("br-dy-stream-msg");
    if (msg) msg.innerHTML = `<span class="err-line">${esc((body && body.message) || `HTTP ${status}`)}</span>`;
    return;
  }
  await loadAll();
}

// The Scan/Query Index selector (ADR 0041) — "— base table —" plus one
// option per declared secondary index; picking one adds `IndexName` to the
// Scan/Query payload sent to `POST /admin/data/dynamo` (see
// `buildQueryPayload`/`runDynamoOp` below). A non-`Active` index (ADR 0045)
// is listed but `disabled` — a Query/Scan against it fails server-side
// (`run_index_query`/`run_index_scan`'s `ValidationException`), so a
// disabled option with a status suffix tells the user why up front instead
// of letting them click "Run" into an error.
function renderIndexSelector(schema) {
  const sel = $("br-dy-index");
  const indexes = (schema && schema.indexes) || [];
  if (!indexes.some((i) => i.name === dyIndexName)) dyIndexName = "";
  sel.innerHTML = `<option value="">— base table —</option>`
    + indexes.map((i) => {
        const status = i.status || "Active";
        const suffix = status === "Creating" ? " — backfilling" : status === "Deleting" ? " — deleting" : "";
        return `<option value="${esc(i.name)}"${i.name === dyIndexName ? " selected" : ""}${status === "Active" ? "" : " disabled"}>${esc(i.name)} (${indexKindLabel(i.kind)})${esc(suffix)}</option>`;
      }).join("");
  sel.value = dyIndexName;
  sel.disabled = !indexes.length;
  // GSI reads are DynamoDB's own eventually-consistent contract (the drain
  // materializes asynchronously); an LSI stays strongly consistent, so the
  // note only ever fires for a Global index.
  const idx = activeIndexDef(schema);
  $("br-dy-index-note").textContent = idx && idx.kind === "Global" ? "eventually consistent (GSI)" : "";
}
function onDyIndexChange() {
  dyIndexName = $("br-dy-index").value;
  renderDynamoFields();
  runDynamoOp();
}

// How far a `Creating` index's backfill has gotten (ADR 0045 §4/§7):
// the only *honest* progress fact is "N of M tablets have reported done" —
// M is the base table's own tablet count *right now* (`tabletsForTable`,
// dashboard_streams.js — the backfill seeder runs on base tablets, ADR 0045
// §2), N is how many of those tablet ids have a matching row in
// `Metadata::index_backfill` (`status.index_backfill`, a flat `{tablet,
// index}` array, PR2's tuple-key codec). No percentage, no interpolation —
// a tablet either has reported or hasn't.
function indexBackfillProgress(table, indexName) {
  const tabletIds = new Set(tabletsForTable(table));
  const rows = (STATE.status && STATE.status.index_backfill) || [];
  let done = 0;
  for (const r of rows) if (r.index === indexName && tabletIds.has(r.tablet)) done++;
  return { done, total: tabletIds.size };
}

// One index's Status cell (ADR 0045/ADR 0021 §7): `Creating` is a normal
// transitional state, not a warning — rendered with the same neutral
// `forming` pill the Tablets view uses for its own transitional state, plus
// the one real progress fact above. `Active` is the steady-state default,
// deliberately quiet (no colored pill) so it doesn't compete for attention
// with the transitional states. `Deleting` is dimmed — it's on its way out.
function indexStatusCell(table, i) {
  const status = i.status || "Active";
  if (status === "Creating") {
    const { done, total } = indexBackfillProgress(table, i.name);
    const progress = total > 0 ? `${done} of ${total} tablets` : "awaiting tablets";
    return `${pill("forming", "Creating")} <span class="muted">backfilling — ${esc(progress)}</span>`;
  }
  if (status === "Deleting") return `<span class="muted" style="opacity:.6">Deleting</span>`;
  return `<span class="muted">Active</span>`;
}

// The table's declared secondary indexes (ADR 0041) plus their real
// lifecycle status and backfill progress (ADR 0045) — ground-truth-only,
// rendered straight off the replicated catalog with nothing inferred or
// faked — and the add/drop-index actions (`UpdateTable`'s
// `GlobalSecondaryIndexUpdates`, ADR 0045 §6). The "+ Add index" trigger and
// each row's "Drop" button are rebuilt here every render (like the Stream
// row's Enable/Disable buttons below) — stateless controls, no typed input
// to lose; the form that DOES hold typed input (`#br-dy-ix-form`) is static
// markup in dashboard.html, touched only by `open`/`closeAddIndexForm`, so a
// routine poll refresh never wipes an in-progress "Add index" form the same
// way `#br-dy-table-form`/`#br-dy-item-form` already don't.
function renderIndexesSection(schema) {
  const el = $("br-dy-indexes");
  if (!schema) { el.innerHTML = ""; return; }
  const table = dyTable;
  const indexes = schema.indexes || [];
  const rows = indexes.map((i) => {
    const status = i.status || "Active";
    const dim = status === "Deleting" ? ' style="opacity:.6"' : "";
    const drop = i.kind === "Global" && status !== "Deleting"
      ? `<button class="danger-text ix-drop" data-name="${esc(i.name)}">Drop</button>`
      : "";
    return `<tr${dim}>
      <td class="mono">${esc(i.name)}</td>
      <td>${pill("forming", indexKindLabel(i.kind))}</td>
      <td class="mono">${esc(i.hash_attribute)}</td>
      <td class="mono">${i.sort_attribute ? esc(i.sort_attribute) : `<span class="muted">—</span>`}</td>
      <td>${esc(projectionLabel(i.projection))}</td>
      <td>${indexStatusCell(table, i)}</td>
      <td>${drop}</td>
    </tr>`;
  }).join("");
  el.innerHTML = `<div class="card scroll" style="margin-top:2px">
    <div class="row" style="justify-content:space-between">
      <h2>Indexes</h2>
      <div class="row"><span class="muted" id="br-dy-ix-drop-msg"></span><button id="br-dy-ix-new">+ Add index</button></div>
    </div>
    ${indexes.length
      ? `<table><thead><tr><th>Name</th><th>Kind</th><th>Hash key</th><th>Sort key</th><th>Projection</th><th>Status</th><th></th></tr></thead>
         <tbody>${rows}</tbody></table>`
      : `<div class="empty">no secondary indexes</div>`}
  </div>`;
  $("br-dy-ix-new").addEventListener("click", openAddIndexForm);
  document.querySelectorAll(".ix-drop").forEach((b) =>
    b.addEventListener("click", () => dropIndex(b.dataset.name)));
}

// ---- add/drop index (ADR 0045 §6/§5) ----
// GSI only, mirroring real DynamoDB — an LSI can't be added to a populated
// table (`decode_index_updates` rejects a `Local` kind at the wire edge
// too; this form simply never offers one).
function openAddIndexForm() {
  dyIxFormOpen = true;
  $("br-dy-ix-name").value = ""; $("br-dy-ix-hash").value = "";
  $("br-dy-ix-has-sort").checked = false; $("br-dy-ix-sort").value = "";
  $("br-dy-ix-sort-wrap").style.display = "none";
  $("br-dy-ix-proj").value = "ALL";
  $("br-dy-ix-include").value = ""; $("br-dy-ix-include-wrap").style.display = "none";
  $("br-dy-ix-msg").textContent = "";
  $("br-dy-ix-form").style.display = "";
}
function closeAddIndexForm() {
  dyIxFormOpen = false;
  $("br-dy-ix-form").style.display = "none";
}
async function submitAddIndexForm() {
  const table = dyTable;
  const name = $("br-dy-ix-name").value.trim();
  const hash = $("br-dy-ix-hash").value.trim();
  const sort = $("br-dy-ix-has-sort").checked ? $("br-dy-ix-sort").value.trim() : "";
  const projType = $("br-dy-ix-proj").value;
  if (!name || !hash) { $("br-dy-ix-msg").textContent = "index name and hash attribute are required"; return; }
  const keySchema = [{ AttributeName: hash, KeyType: "HASH" }];
  const attrDefs = [{ AttributeName: hash, AttributeType: "S" }];
  if (sort) {
    keySchema.push({ AttributeName: sort, KeyType: "RANGE" });
    attrDefs.push({ AttributeName: sort, AttributeType: "S" });
  }
  const create = { IndexName: name, KeySchema: keySchema };
  if (projType === "INCLUDE") {
    const names = $("br-dy-ix-include").value.split(",").map((s) => s.trim()).filter(Boolean);
    if (!names.length) { $("br-dy-ix-msg").textContent = "INCLUDE projection needs at least one attribute"; return; }
    create.Projection = { ProjectionType: "INCLUDE", NonKeyAttributes: names };
  } else if (projType === "KEYS_ONLY") {
    create.Projection = { ProjectionType: "KEYS_ONLY" };
  } // ALL: omit `Projection` (the wire decoder's own default)
  $("br-dy-ix-msg").textContent = "adding…";
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
      op: "UpdateTable",
      payload: {
        TableName: table,
        AttributeDefinitions: attrDefs,
        GlobalSecondaryIndexUpdates: [{ Create: create }],
      },
    });
    if (status >= 300) { $("br-dy-ix-msg").textContent = (body && body.message) || ("HTTP " + status); return; }
    closeAddIndexForm();
    await loadAll();
  } catch (e) { $("br-dy-ix-msg").textContent = String(e); }
}
async function dropIndex(name) {
  const table = dyTable;
  if (!window.confirm(`Drop index “${name}” on “${table}”? This deletes the index's materialized data — it cannot be undone.`)) return;
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "UpdateTable",
    payload: { TableName: table, GlobalSecondaryIndexUpdates: [{ Delete: { IndexName: name } }] },
  });
  if (status >= 300) {
    const msg = $("br-dy-ix-drop-msg");
    if (msg) msg.innerHTML = `<span class="err-line">${esc((body && body.message) || `HTTP ${status}`)}</span>`;
    return;
  }
  await loadAll();
}

function setDynamoOp(op) {
  dyOp = op;
  $("br-dy-op-scan").classList.toggle("active", op === "scan");
  $("br-dy-op-query").classList.toggle("active", op === "query");
  $("br-dy-query-form").style.display = op === "query" ? "" : "none";
  $("br-dy-scan-form").style.display = op === "scan" ? "" : "none";
}

// `pk = :pk [AND sk = :sk | AND sk BETWEEN :lo AND :hi | AND begins_with(sk, :sk)]`
// — the exact grammar animus_dynamo's Query decoder supports (wire.rs).
// `pk`/`sk` are the *base table's* key when no index is selected, or the
// selected index's own hash/sort attribute (`activeKeyNames`, ADR 0041) —
// `IndexName` rides along in the payload whenever one is.
function buildQueryPayload(schema) {
  const [pk, sk] = activeKeyNames(schema);
  let expr = `${pk} = :pk`;
  const values = { ":pk": attrValue({ type: dynColType(schema, pk), value: dyPkValue }) };
  if (sk && dySkValue.trim()) {
    const skType = dynColType(schema, sk);
    if (dySkOp === "begins_with") {
      expr += ` AND begins_with(${sk}, :sk)`;
      values[":sk"] = attrValue({ type: skType, value: dySkValue });
    } else if (dySkOp === "between") {
      const [a, b] = dySkValue.split(",").map((x) => x.trim());
      expr += ` AND ${sk} BETWEEN :lo AND :hi`;
      values[":lo"] = attrValue({ type: skType, value: a || "" });
      values[":hi"] = attrValue({ type: skType, value: b || "" });
    } else {
      expr += ` AND ${sk} = :sk`;
      values[":sk"] = attrValue({ type: skType, value: dySkValue });
    }
  }
  const payload = { TableName: dyTable, KeyConditionExpression: expr, ExpressionAttributeValues: values };
  if (dyIndexName) payload.IndexName = dyIndexName;
  return payload;
}

async function runDynamoOp() {
  if (!dyTable) { dyResultItems = []; dyResultError = null; dyResultSummary = ""; renderDynamoResults(); return; }
  const schema = dynamoTables()[dyTable];
  dySelectedIndex = null;
  try {
    let op, payload;
    if (dyOp === "query") {
      if (!dyPkValue.trim()) {
        dyResultItems = []; dyResultError = null;
        dyResultSummary = "Enter a partition key value and run the query.";
        renderDynamoResults();
        return;
      }
      op = "Query";
      payload = buildQueryPayload(schema);
    } else {
      op = "Scan";
      payload = { TableName: dyTable, Limit: 50 };
      if (dyIndexName) payload.IndexName = dyIndexName;
    }
    const { status, body } = await postJSON(SEED, "/admin/data/dynamo", { op, payload });
    if (status >= 300) {
      dyResultItems = []; dyResultError = (body && body.message) || ("HTTP " + status);
      renderDynamoResults();
      return;
    }
    let items = body.Items || [];
    if (dyOp === "scan" && dyFilterAttr && dyFilterValue.trim()) {
      const fv = dyFilterValue.trim().toLowerCase();
      items = items.filter((it) => avDecode(it[dyFilterAttr]).toLowerCase().includes(fv));
    }
    dyResultItems = items;
    dyResultError = null;
    dyResultSummary = dyOp === "query"
      ? `${items.length} item(s) returned`
      : `${body.Count != null ? body.Count : items.length} scanned · ${items.length} shown (filtered on this page only)`;
  } catch (e) {
    dyResultItems = []; dyResultError = String(e);
  }
  renderDynamoResults();
}

function renderDynamoResults() {
  const columns = [...new Set(dyResultItems.flatMap((it) => Object.keys(it)))];
  const schema = dynamoTables()[dyTable];
  const keyNames = schema ? dynKeyNames(schema) : [];
  columns.sort((a, b) => {
    const ra = keyNames.indexOf(a), rb = keyNames.indexOf(b);
    if (ra !== -1 || rb !== -1) return (ra === -1 ? 99 : ra) - (rb === -1 ? 99 : rb);
    return a.localeCompare(b);
  });

  const filterSel = $("br-dy-filter-attr");
  const prevF = filterSel.value || dyFilterAttr;
  filterSel.innerHTML = `<option value="">none</option>`
    + columns.map((c) => `<option${c === prevF ? " selected" : ""}>${esc(c)}</option>`).join("");
  dyFilterAttr = columns.includes(prevF) ? prevF : "";
  filterSel.value = dyFilterAttr;

  $("br-dy-summary").innerHTML = dyResultError ? `<span class="err-line">${esc(dyResultError)}</span>` : esc(dyResultSummary);
  $("br-dy-new-item").disabled = !dyTable;

  if (!dyResultItems.length) {
    $("br-dy-items-table").innerHTML = `<tbody><tr><td class="empty">${dyResultError ? "" : "no items"}</td></tr></tbody>`;
  } else {
    const head = columns.map((c) => `<th>${esc(c)}</th>`).join("") + `<th>Actions</th>`;
    const rows = dyResultItems.map((it, idx) => {
      const cells = columns.map((c) => `<td class="mono" style="font-size:11.5px">${esc(avDecode(it[c]))}</td>`).join("");
      return `<tr class="clickable${dySelectedIndex === idx ? " selected" : ""}" data-idx="${idx}">${cells}
        <td><button class="link-text ia-edit" data-idx="${idx}">Edit</button>
        <button class="danger-text ia-delete" data-idx="${idx}" style="margin-left:10px">Delete</button></td></tr>`;
    }).join("");
    $("br-dy-items-table").innerHTML = `<thead><tr>${head}</tr></thead><tbody>${rows}</tbody>`;
    document.querySelectorAll("#br-dy-items-table tr[data-idx]").forEach((tr) =>
      tr.addEventListener("click", (e) => {
        if (e.target.closest("button")) return;
        const idx = Number(tr.dataset.idx);
        dySelectedIndex = dySelectedIndex === idx ? null : idx;
        renderDynamoResults();
      }));
    document.querySelectorAll(".ia-edit").forEach((b) =>
      b.addEventListener("click", (e) => { e.stopPropagation(); openEditItemForm(Number(b.dataset.idx)); }));
    document.querySelectorAll(".ia-delete").forEach((b) =>
      b.addEventListener("click", (e) => { e.stopPropagation(); deleteItem(Number(b.dataset.idx)); }));
  }

  if (dySelectedIndex != null && dyResultItems[dySelectedIndex]) {
    const plain = {};
    Object.keys(dyResultItems[dySelectedIndex]).forEach((k) => { plain[k] = avDecode(dyResultItems[dySelectedIndex][k]); });
    $("br-dy-item-detail").innerHTML = `<div class="row" style="justify-content:space-between;margin-bottom:10px">
      <span style="font:600 12px var(--font-ui)">Item detail</span>
      <button class="link-text" id="br-dy-item-detail-close">Close ×</button></div>
      <pre>${esc(JSON.stringify(plain, null, 2))}</pre>`;
    $("br-dy-item-detail").style.display = "";
    $("br-dy-item-detail-close").addEventListener("click", () => { dySelectedIndex = null; renderDynamoResults(); });
  } else {
    $("br-dy-item-detail").style.display = "none";
  }
}

// ---- item create/edit form: a dynamic attribute-row editor (name/type/value,
// add/remove), key attributes locked — DynamoDB items are schemaless beyond
// their declared keys, so a fixed-column form (as in the design mockup, whose
// fake table had fixed columns) can't represent arbitrary real items. ----
function addItemAttrRow(name = "", type = "S", value = "", locked = false) {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td><input type="text" class="ia-n" value="${esc(name)}"${locked ? " readonly" : ""}></td>
    <td><select class="ia-t"${locked ? " disabled" : ""}>
      <option ${type === "S" ? "selected" : ""}>S</option><option ${type === "N" ? "selected" : ""}>N</option>
      <option ${type === "B" ? "selected" : ""}>B</option><option ${type === "BOOL" ? "selected" : ""}>BOOL</option></select></td>
    <td><input type="text" class="ia-v" value="${esc(value)}"></td>
    <td>${locked ? `<span class="key-badge" title="key attribute">key</span>` : `<button class="danger-text ia-del">✕</button>`}</td>`;
  const del = tr.querySelector(".ia-del");
  if (del) del.addEventListener("click", () => tr.remove());
  $("br-dy-item-attrs").querySelector("tbody").appendChild(tr);
}
function itemAttrRows() {
  return [...$("br-dy-item-attrs").querySelectorAll("tbody tr")].map((tr) => ({
    name: tr.querySelector(".ia-n").value.trim(),
    type: tr.querySelector(".ia-t").value,
    value: tr.querySelector(".ia-v").value,
  })).filter((a) => a.name);
}
function openCreateItemForm() {
  const schema = dynamoTables()[dyTable];
  if (!schema) return;
  dyItemFormMode = "create"; dyItemFormOpen = true;
  renderItemForm(schema, {});
}
function openEditItemForm(idx) {
  const schema = dynamoTables()[dyTable];
  if (!schema) return;
  dyItemFormMode = "edit"; dyItemFormOpen = true;
  renderItemForm(schema, dyResultItems[idx]);
}
function renderItemForm(schema, item) {
  $("br-dy-item-form-title").textContent = dyItemFormMode === "edit" ? "Edit item" : "Create item";
  $("br-dy-item-attrs").querySelector("tbody").innerHTML = "";
  const keyNames = dynKeyNames(schema);
  keyNames.forEach((name) => {
    const v = item[name];
    addItemAttrRow(name, v ? avType(v) : dynColType(schema, name), v ? avDecode(v) : "", true);
  });
  Object.keys(item).filter((k) => !keyNames.includes(k)).forEach((k) => addItemAttrRow(k, avType(item[k]), avDecode(item[k]), false));
  if (Object.keys(item).length === keyNames.length) addItemAttrRow();
  $("br-dy-item-form").style.display = "";
}
function closeItemForm() {
  dyItemFormOpen = false;
  $("br-dy-item-form").style.display = "none";
}
async function submitItemForm() {
  const rows = itemAttrRows();
  const av = {};
  rows.forEach((a) => { av[a.name] = attrValue(a); });
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", { op: "PutItem", payload: { TableName: dyTable, Item: av } });
  if (status >= 300) { $("br-dy-summary").innerHTML = `<span class="err-line">${esc((body && body.message) || "HTTP " + status)}</span>`; return; }
  closeItemForm();
  await runDynamoOp();
}
async function deleteItem(idx) {
  const schema = dynamoTables()[dyTable];
  const item = dyResultItems[idx];
  if (!window.confirm("Delete this item?")) return;
  const key = {};
  dynKeyNames(schema).forEach((k) => { key[k] = item[k]; });
  await postJSON(SEED, "/admin/data/dynamo", { op: "DeleteItem", payload: { TableName: dyTable, Key: key } });
  dySelectedIndex = null;
  await runDynamoOp();
}

// ---- create/drop table ----
function openTableForm() {
  dyTableFormOpen = true;
  $("br-dy-ct-name").value = ""; $("br-dy-ct-pk").value = "id"; $("br-dy-ct-pkt").value = "S";
  $("br-dy-ct-has-sk").checked = false; $("br-dy-ct-sk").value = ""; $("br-dy-ct-skt").value = "S";
  $("br-dy-ct-sk-wrap").style.display = "none"; $("br-dy-ct-skt-wrap").style.display = "none";
  $("br-dy-ct-msg").textContent = "";
  $("br-dy-table-form").style.display = "";
}
function closeTableForm() { dyTableFormOpen = false; $("br-dy-table-form").style.display = "none"; }
async function submitTableForm() {
  const name = $("br-dy-ct-name").value.trim();
  const pk = $("br-dy-ct-pk").value.trim();
  const sk = $("br-dy-ct-has-sk").checked ? $("br-dy-ct-sk").value.trim() : "";
  if (!name || !pk) { $("br-dy-ct-msg").textContent = "name and partition key are required"; return; }
  const keySchema = [{ AttributeName: pk, KeyType: "HASH" }];
  const attrDefs = [{ AttributeName: pk, AttributeType: $("br-dy-ct-pkt").value }];
  if (sk) {
    keySchema.push({ AttributeName: sk, KeyType: "RANGE" });
    attrDefs.push({ AttributeName: sk, AttributeType: $("br-dy-ct-skt").value });
  }
  $("br-dy-ct-msg").textContent = "creating…";
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/dynamo",
      { op: "CreateTable", payload: { TableName: name, KeySchema: keySchema, AttributeDefinitions: attrDefs } });
    if (status >= 300) { $("br-dy-ct-msg").textContent = (body && body.message) || ("HTTP " + status); return; }
    dyTable = name;
    closeTableForm();
    await loadAll();
  } catch (e) { $("br-dy-ct-msg").textContent = String(e); }
}
async function dropCurrentTable() {
  if (!dyTable) return;
  if (!window.confirm(`Drop table “${dyTable}”? Its schema is removed (existing rows are not garbage-collected).`)) return;
  await postJSON(SEED, "/admin/data/drop-table", { table: dyTable });
  await loadAll();
}

// ---- Bulk seed (sharding test) ----
let seeding = false;
let seedTable = "";

// The seeder writes real DynamoDB items (keyed by the table's catalog schema),
// so the target list is the DynamoDB tables (`dynamoTables()`) that *have a
// tablet*, the set `/admin/data/seed` accepts (it validates against the
// replicated tablet map).
// Refreshed by render() (which also runs mid-seed via loadAll), so keep the
// Seed button's disabled state in sync with both table validity and `seeding`.
// Auto-picks the first seedable table when none is selected yet or the
// current selection no longer qualifies.
function renderSeedTables() {
  const tablets = (STATE.status && STATE.status.tablets) || {};
  const withTablet = new Set(Object.values(tablets).map((t) => t.table).filter(Boolean));
  const seedable = Object.keys(dynamoTables()).filter((n) => withTablet.has(n)).sort();
  if (!seedable.includes(seedTable)) seedTable = seedable[0] || "";
  const sel = $("seed-table");
  sel.innerHTML = seedable.length
    ? seedable.map((n) => `<option${n === seedTable ? " selected" : ""}>${esc(n)}</option>`).join("")
    : `<option value="">(none)</option>`;
  sel.value = seedTable;
  const validTable = !!seedTable;
  $("seed-no-tables").style.display = validTable ? "none" : "";
  $("seed-no-tables").textContent = seedable.length
    ? ""
    : "no DynamoDB table has a tablet yet — create one above";
  $("seed-table").disabled = !seedable.length;
  $("seed-go").disabled = !validTable || seeding;
}

async function seedRun() {
  if (seeding) return;
  const table = seedTable;
  const prefix = $("seed-prefix").value;
  const vbytes = Math.max(0, parseInt($("seed-vbytes").value, 10) || 0);
  const total = Math.max(0, parseInt($("seed-total").value, 10) || 0);
  const chunk = Math.max(1, parseInt($("seed-chunk").value, 10) || 1000);
  if (!table) { $("seed-status").innerHTML = `<div class="empty">select a table above — every key names a table</div>`; return; }
  if (!total) { $("seed-status").innerHTML = `<div class="empty">set a total</div>`; return; }
  seeding = true;
  $("seed-go").disabled = true;
  $("seed-stop").disabled = false;
  let done = 0;
  const t0 = Date.now();
  // Refresh the Tablets view live so splits appear during a large seed — but
  // NEVER block a seed chunk on it. loadAll() serializes a full cluster-wide
  // admin fan-out; awaiting it every chunk would fold that latency into the
  // displayed keys/s rate and throttle throughput. Instead fire it non-blocking
  // and at most once per REFRESH_MS of wall-clock, and drop overlapping refreshes.
  const REFRESH_MS = 1000;
  let lastRefresh = 0;
  let refreshInFlight = false;
  const liveRefresh = () => {
    if (refreshInFlight) return;
    const now = Date.now();
    if (now - lastRefresh < REFRESH_MS) return;
    lastRefresh = now;
    refreshInFlight = true;
    loadAll().finally(() => { refreshInFlight = false; });
  };
  try {
    while (done < total && seeding) {
      const count = Math.min(chunk, total - done);
      const { status, body } = await postJSON(SEED, "/admin/data/seed",
        { table, count, start: done, key_prefix: prefix, value_bytes: vbytes });
      if (status >= 300) {
        $("seed-status").innerHTML = `<div class="err-line">${esc((body && body.error) || ("HTTP " + status))}</div>`;
        break;
      }
      const wrote = body.written || 0;
      done += wrote;
      const secs = Math.max(0.001, (Date.now() - t0) / 1000);
      const rate = Math.round(done / secs);
      $("seed-status").innerHTML =
        `<div class="muted">seeded ${done.toLocaleString()} / ${total.toLocaleString()} items · ${rate.toLocaleString()}/s`
        + (body.error ? ` · <span class="err-line">last error: ${esc(body.error)}</span>` : "") + `</div>`;
      if (wrote === 0) break; // persistent failure — don't spin
      liveRefresh(); // non-blocking, throttled — splits show live without gating throughput
    }
    await loadAll(); // final authoritative refresh once seeding settles
    if (done >= total) $("seed-status").innerHTML += `<div>${pill("ok", "done — " + done.toLocaleString() + " items")}</div>`;
    else if (!seeding) $("seed-status").innerHTML += `<div class="muted">stopped at ${done.toLocaleString()}</div>`;
  } catch (e) {
    $("seed-status").innerHTML = `<div class="err-line">${esc(e)}</div>`;
  } finally {
    seeding = false;
    renderSeedTables(); // recompute seed-go (stays disabled if no tables remain)
    $("seed-stop").disabled = true;
  }
}
