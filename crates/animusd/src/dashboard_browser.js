"use strict";
// The Data Browser view: a CQL query box + results, and a DynamoDB panel
// (table select, Scan/Query, an item list with per-row Edit/Delete, a detail
// panel, Create table / Create item forms, and the bulk-seed tool — which
// writes real DynamoDB items, hence lives here rather than in Storage) — all
// against the real /admin/data/{cql,dynamo,drop-table,seed} endpoints, not
// the design mockup's fake in-memory item store. Depends on
// `dashboard_core.js` (STATE, $, esc, pill, getJSON, postJSON, loadAll).

let brProtocol = "cql";

// ---- DynamoDB: schema helpers (shared by Scan/Query/item forms) ----
function dynamoTables() {
  const t = STATE.status && STATE.status.schemas && STATE.status.schemas.tables;
  if (!t) return {};
  const out = {};
  for (const k of Object.keys(t)) if (!k.includes(".")) out[k] = t[k];
  return out;
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
let dySelectedIndex = null;
let dyResultItems = []; // raw AttributeValue-map items from the last successful Scan/Query
let dyResultSummary = "";
let dyResultError = null;
let dyTableFormOpen = false;
let dyItemFormOpen = false;
let dyItemFormMode = "create";

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
  dySelectedIndex = null; dyResultItems = []; dyResultError = null;
  closeItemForm();
  renderDynamoFields();
  setDynamoOp("scan");
  if (dyTable) runDynamoOp(); else renderDynamoResults();
}

function renderDynamoFields() {
  const schema = dynamoTables()[dyTable];
  $("br-dy-fields").innerHTML = !schema ? "" : `
    <div class="field"><div class="k">Partition key</div><div class="v">${esc(schema.partition_key || "—")} <span class="muted">(${esc(dynColType(schema, schema.partition_key))})</span></div></div>
    ${schema.clustering_keys && schema.clustering_keys[0]
      ? `<div class="field"><div class="k">Sort key</div><div class="v">${esc(schema.clustering_keys[0])} <span class="muted">(${esc(dynColType(schema, schema.clustering_keys[0]))})</span></div></div>`
      : ""}`;
  const hasSk = !!(schema && schema.clustering_keys && schema.clustering_keys[0]);
  $("br-dy-query-sk-group").style.display = hasSk ? "flex" : "none";
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
function buildQueryPayload(schema) {
  const pk = schema.partition_key;
  let expr = `${pk} = :pk`;
  const values = { ":pk": attrValue({ type: dynColType(schema, pk), value: dyPkValue }) };
  const sk = schema.clustering_keys && schema.clustering_keys[0];
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
  return { TableName: dyTable, KeyConditionExpression: expr, ExpressionAttributeValues: values };
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

// ---- CQL ----
function setProtocol(p) {
  brProtocol = p;
  $("br-proto-cql").classList.toggle("active", p === "cql");
  $("br-proto-dynamo").classList.toggle("active", p === "dynamo");
  $("br-cql-panel").style.display = p === "cql" ? "" : "none";
  $("br-dynamo-panel").style.display = p === "dynamo" ? "" : "none";
}
async function runCql() {
  const query = $("br-cql-query").value;
  const ks = $("br-cql-ks").value.trim();
  if (!query.trim()) { $("br-cql-result").innerHTML = `<div class="empty">enter a statement</div>`; return; }
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/cql", { query, keyspace: ks || null });
    if (status !== 200 || !body.results) {
      $("br-cql-result").innerHTML = `<div class="err-line">${esc((body && body.error) || ("HTTP " + status))}</div>`;
      return;
    }
    $("br-cql-result").innerHTML = body.results.map(renderCqlResult).join("");
  } catch (e) { $("br-cql-result").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}
function renderCqlResult(r) {
  const head = `<div class="muted" style="margin-top:8px"><code>${esc(r.statement)}</code></div>`;
  if (r.kind === "error") return head + `<div class="err-line">${esc(r.error)}</div>`;
  if (r.kind === "rows") {
    const cols = (r.columns || []).map((c) => `<th>${esc(c)}</th>`).join("");
    const rows = (r.rows || []).map((row) =>
      `<tr>${row.map((c) => `<td class="mono">${c === null ? "<span class='muted'>null</span>" : esc(c)}</td>`).join("")}</tr>`).join("");
    return head + `<table><thead><tr>${cols}</tr></thead><tbody>${rows}</tbody></table>`
      + `<div class="muted">${esc(r.row_count)} row(s)</div>`;
  }
  if (r.kind === "set_keyspace") return head + `<div class="muted">${pill("ok", "USE " + esc(r.keyspace))}</div>`;
  if (r.kind === "schema_change") return head + `<div class="muted">${pill("ok", esc(r.change) + " " + esc(r.target))}</div>`;
  return head + `<div class="muted">${pill("ok", "ok")}</div>`;
}

// ---- Bulk seed (sharding test) ----
let seeding = false;
let seedTable = "";

// The seeder writes real DynamoDB items (keyed by the table's catalog schema),
// so the target list is the DynamoDB tables (`dynamoTables()` — the catalog
// minus CQL `ks.table` entries) that *have a tablet*, the set
// `/admin/data/seed` accepts (it validates against the replicated tablet map).
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
