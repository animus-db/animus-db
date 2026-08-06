"use strict";
// The Write tab: DynamoDB table management + operation form/JSON editor, the
// CQL script runner, and bulk seed. Depends on `dashboard_core.js` having
// loaded first (STATE, $, esc, pill, getJSON, postJSON, loadAll).

// ---- Write tab: DynamoDB ----
// A single request model (`dynModel`) is the source of truth; the Form and JSON
// views read/write it and the toggle syncs between them. The op + table row is
// shared by both views; the table is a dropdown of *existing* tables only.
let dynModel = { TableName: "", Item: {} };
let dynView = "form";
const DY_ATTR_OPS = ["PutItem", "GetItem", "DeleteItem", "UpdateItem"];

// The operation panel's own table selection (its `#dy-table` dropdown) —
// each write-tab panel keeps its own local selection now rather than sharing
// one global header dropdown. `lastRenderedDyTable` tracks the value the
// editor was last built for, so a routine refresh that leaves the selection
// unchanged doesn't clobber in-progress edits (checked in core.js's `render`).
let dyTable = "";
let lastRenderedDyTable;

// The Dynamo tables known to the cluster (from the replicated catalog in
// /admin/status), keyed by name. Dynamo tables are plain-named; CQL tables are
// `ks.table`, so we filter those out.
function dynamoTables() {
  const t = STATE.status && STATE.status.schemas && STATE.status.schemas.tables;
  if (!t) return {};
  const out = {};
  for (const k of Object.keys(t)) if (!k.includes(".")) out[k] = t[k];
  return out;
}

function addAttrRow(name = "", type = "S", value = "", locked = false) {
  const tr = document.createElement("tr");
  tr.innerHTML = `<td><input type="text" class="dy-n" value="${esc(name)}"${locked ? " readonly" : ""}></td>
    <td><select class="dy-t"${locked ? " disabled" : ""}><option ${type==="S"?"selected":""}>S</option>
      <option ${type==="N"?"selected":""}>N</option>
      <option ${type==="B"?"selected":""}>B</option>
      <option ${type==="BOOL"?"selected":""}>BOOL</option></select></td>
    <td><input type="text" class="dy-v" value="${esc(value)}"></td>
    <td>${locked
      ? `<span class="muted dy-key-lock" title="key attribute — required by the table's schema">key</span>`
      : `<a href="#" class="dy-del">✕</a>`}</td>`;
  const del = tr.querySelector(".dy-del");
  if (del) del.addEventListener("click", (e) => { e.preventDefault(); tr.remove(); });
  $("dy-attrs").querySelector("tbody").appendChild(tr);
}
function dyAttrs() {
  return [...$("dy-attrs").querySelectorAll("tbody tr")].map((tr) => ({
    name: tr.querySelector(".dy-n").value.trim(),
    type: tr.querySelector(".dy-t").value,
    value: tr.querySelector(".dy-v").value,
  })).filter((a) => a.name);
}
function attrValue(a) {
  if (a.type === "N") return { N: a.value };
  if (a.type === "B") return { B: a.value };
  if (a.type === "BOOL") return { BOOL: a.value === "true" || a.value === "1" };
  return { S: a.value };
}
function currentAv() {
  const av = {}; dyAttrs().forEach((a) => { av[a.name] = attrValue(a); }); return av;
}
function avToAttrs(av) {
  if (!av || typeof av !== "object") return [];
  return Object.entries(av).map(([name, val]) => {
    const type = val.N != null ? "N" : (val.B != null ? "B" : (val.BOOL != null ? "BOOL" : "S"));
    const value = val.N != null ? val.N
      : (val.B != null ? val.B : (val.BOOL != null ? String(val.BOOL) : (val.S != null ? val.S : "")));
    return { name, type, value };
  });
}

// The DynamoDB attribute type (S/N/B) for a key column, from the table's schema.
function dynColType(schema, name) {
  const col = (schema.columns || []).find((c) => c.name === name);
  const ty = col && col.ty;
  return ty === "Number" ? "N" : (ty === "Binary" ? "B" : "S");
}
// The table's key attributes as an AttributeValue map with empty values, ready to
// prefill an Item/Key — partition key, then sort key (first clustering key) if any.
function dynKeyAv(table) {
  const s = dynamoTables()[table];
  const av = {};
  if (!s) return av;
  const put = (name) => { av[name] = attrValue({ type: dynColType(s, name), value: "" }); };
  if (s.partition_key) put(s.partition_key);
  const sk = s.clustering_keys && s.clustering_keys[0];
  if (sk) put(sk);
  return av;
}
// The table's key attribute names (partition key, then sort key if any). These
// rows are locked in the form — a valid Item/Key must carry them.
function dynKeyNames(table) {
  const s = dynamoTables()[table];
  if (!s) return [];
  const names = [];
  if (s.partition_key) names.push(s.partition_key);
  const sk = s.clustering_keys && s.clustering_keys[0];
  if (sk) names.push(sk);
  return names;
}
// The partition key name (for prefilling a Query's KeyConditionExpression).
function dynPartitionKey(table) {
  const s = dynamoTables()[table];
  return (s && s.partition_key) || "pk";
}

// A fresh request skeleton for `op` against `table`, with the table's known key
// attributes prefilled (the partition key, and sort key if any).
function dynSkeleton(op, table) {
  const keyAv = dynKeyAv(table);
  switch (op) {
    case "PutItem": return { TableName: table, Item: { ...keyAv } };
    case "GetItem": return { TableName: table, Key: { ...keyAv } };
    case "DeleteItem": return { TableName: table, Key: { ...keyAv } };
    case "UpdateItem": return { TableName: table, Key: { ...keyAv },
      UpdateExpression: "SET v = :v", ExpressionAttributeValues: { ":v": { S: "new" } } };
    case "Query": {
      const pk = dynPartitionKey(table);
      return { TableName: table, KeyConditionExpression: `${pk} = :${pk}`,
        ExpressionAttributeValues: { [":" + pk]: { S: "..." } } };
    }
    case "Scan": return { TableName: table, Limit: 25 };
    default: return { TableName: table };
  }
}

// Capture the active view's edits into dynModel.
function dynSyncFromForm() {
  dynModel.TableName = dyTable;
  const op = $("dy-op").value;
  if (op === "PutItem") dynModel.Item = currentAv();
  else if (DY_ATTR_OPS.includes(op)) dynModel.Key = currentAv();
  // Query/Scan have no attr form — their fields live only in the JSON view.
}
function dynSyncFromJson() { dynModel = JSON.parse($("dy-raw").value); }

// Render the active view from dynModel.
function dynRender() {
  const op = $("dy-op").value;
  const usesAttrs = DY_ATTR_OPS.includes(op);
  const table = dyTable;
  const validTable = !!(table && dynamoTables()[table]);
  $("dy-view-form").classList.toggle("active", dynView === "form");
  $("dy-view-json").classList.toggle("active", dynView === "json");
  if (dynView === "json") {
    $("dy-form").style.display = "none";
    $("dy-json").style.display = "";
    $("dy-raw").value = JSON.stringify(dynModel, null, 2);
    return;
  }
  $("dy-form").style.display = "";
  $("dy-json").style.display = "none";
  const show = usesAttrs && validTable ? "" : "none";
  $("dy-attrs").style.display = show;
  $("dy-add-attr").style.display = show;
  $("dy-attrs").querySelector("tbody").innerHTML = "";
  if (usesAttrs && validTable) {
    const rows = avToAttrs(op === "PutItem" ? dynModel.Item : dynModel.Key);
    // Key attributes render first as locked rows (name/type fixed, no delete):
    // the Item/Key must carry them. Re-added with an empty value if missing
    // from the model (e.g. pruned in the JSON view).
    const keyNames = dynKeyNames(table);
    const schema = dynamoTables()[table];
    keyNames.forEach((name) => {
      const a = rows.find((r) => r.name === name);
      addAttrRow(name, a ? a.type : dynColType(schema, name), a ? a.value : "", true);
    });
    const rest = rows.filter((r) => !keyNames.includes(r.name));
    rest.forEach((a) => addAttrRow(a.name, a.type, a.value));
    if (!keyNames.length && !rest.length) addAttrRow();
    $("dy-form-hint").textContent = op === "PutItem" ? "attributes = the full Item" : "attributes = the Key";
  } else if (!validTable) {
    $("dy-form-hint").textContent = "pick a DynamoDB table above";
  } else {
    $("dy-form-hint").textContent = "switch to JSON to edit this operation's request";
  }
}

function dynSetView(view) {
  if (view === dynView) return;
  if (view === "json") { dynSyncFromForm(); }
  else {
    try { dynSyncFromJson(); }
    catch (e) { $("dy-result").innerHTML = `<div class="err-line">invalid JSON — fix it before switching: ${esc(e)}</div>`; return; }
  }
  dynView = view;
  dynRender();
}
function dynOnOp() {
  const prevAv = currentAv();
  dynModel = dynSkeleton($("dy-op").value, dyTable);
  const op = $("dy-op").value;
  if (Object.keys(prevAv).length) {
    if (op === "PutItem") dynModel.Item = prevAv;
    else if (DY_ATTR_OPS.includes(op)) dynModel.Key = prevAv;
  }
  dynRender();
}
function dynOnTable() {
  // Switching table rebuilds the skeleton so the new table's key attributes are
  // prefilled (in both form and JSON views).
  dynModel = dynSkeleton($("dy-op").value, dyTable);
  dynRender();
}

// Render the tables management list, and this panel's own `#dy-table`
// selector (replacing the old shared header dropdown — each write-tab panel
// now owns its selection). Auto-picks the first table when none is selected
// yet or the current selection no longer exists, so there's no separate
// "now go pick a table" step in the common single-table case.
function renderDynamoTables() {
  const tables = dynamoTables();
  const names = Object.keys(tables).sort();
  $("dy-tables").innerHTML = names.length
    ? `<table><thead><tr><th>table</th><th>partition key</th><th>sort key</th><th></th></tr></thead><tbody>`
      + names.map((n) => {
          const s = tables[n];
          const sk = (s.clustering_keys && s.clustering_keys[0]) || "";
          return `<tr><td class="mono">${esc(n)}</td><td class="mono">${esc(s.partition_key || "")}</td>
            <td class="mono">${sk ? esc(sk) : "<span class='muted'>—</span>"}</td>
            <td><a href="#" class="dy-drop" data-t="${esc(n)}">drop</a></td></tr>`;
        }).join("")
      + `</tbody></table>`
    : `<div class="empty">no DynamoDB tables — create one above</div>`;
  $("dy-tables").querySelectorAll(".dy-drop").forEach((a) =>
    a.addEventListener("click", (e) => { e.preventDefault(); dropTable(a.dataset.t); }));

  if (!names.includes(dyTable)) dyTable = names[0] || "";
  const sel = $("dy-table");
  sel.innerHTML = names.length
    ? names.map((n) => `<option${n === dyTable ? " selected" : ""}>${esc(n)}</option>`).join("")
    : `<option value="">(none)</option>`;
  sel.value = dyTable;
  const validTable = !!dyTable;
  $("dy-no-tables").style.display = validTable ? "none" : "";
  $("dy-no-tables").textContent = names.length ? "" : "create a table first";
  ["dy-op", "dy-table", "dy-send", "dy-add-attr"].forEach((id) => { $(id).disabled = !validTable; });
}

// The `#dy-table` selector's own change handler — updates immediately (not
// waiting for the next poll refresh) and marks the change as already handled
// so `render()`'s `dyTable !== lastRenderedDyTable` check doesn't redundantly
// rebuild the skeleton a second time.
function onDyTableChange() {
  dyTable = $("dy-table").value;
  lastRenderedDyTable = dyTable;
  dynOnTable();
}

async function createTable() {
  const name = $("dy-ct-name").value.trim();
  const pk = $("dy-ct-pk").value.trim();
  const sk = $("dy-ct-sk").value.trim();
  if (!name || !pk) { $("dy-ct-msg").textContent = "name and partition key are required"; return; }
  const keySchema = [{ AttributeName: pk, KeyType: "HASH" }];
  const attrDefs = [{ AttributeName: pk, AttributeType: $("dy-ct-pkt").value }];
  if (sk) {
    keySchema.push({ AttributeName: sk, KeyType: "RANGE" });
    attrDefs.push({ AttributeName: sk, AttributeType: $("dy-ct-skt").value });
  }
  const payload = { TableName: name, KeySchema: keySchema, AttributeDefinitions: attrDefs };
  $("dy-ct-msg").textContent = "creating…";
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/dynamo", { op: "CreateTable", payload });
    if (status >= 300) { $("dy-ct-msg").textContent = (body && body.message) || ("HTTP " + status); return; }
    $("dy-ct-msg").textContent = "created " + name;
    $("dy-ct-name").value = "";
    dyTable = name; // render() picks this up once /admin/status reflects it, prefilling key attrs
    await loadAll();
  } catch (e) { $("dy-ct-msg").textContent = String(e); }
}
async function dropTable(name) {
  if (!window.confirm(`Drop table “${name}”? Its schema is removed (existing rows are not garbage-collected).`)) return;
  $("dy-ct-msg").textContent = "dropping…";
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/drop-table", { table: name });
    $("dy-ct-msg").textContent = status < 300 ? "dropped " + name : ((body && body.error) || ("HTTP " + status));
    await loadAll();
  } catch (e) { $("dy-ct-msg").textContent = String(e); }
}

async function sendDynamo() {
  try { if (dynView === "form") dynSyncFromForm(); else dynSyncFromJson(); }
  catch (e) { $("dy-result").innerHTML = `<div class="err-line">invalid JSON: ${esc(e)}</div>`; return; }
  if (!dynModel.TableName) { $("dy-result").innerHTML = `<div class="err-line">no table selected</div>`; return; }
  const op = $("dy-op").value;
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/dynamo", { op, payload: dynModel });
    const cls = status < 300 ? "ok" : "err";
    $("dy-result").innerHTML = `<div class="muted">${pill(cls, "HTTP " + status)}</div>`
      + `<pre class="mono" style="white-space:pre-wrap">${esc(JSON.stringify(body, null, 2))}</pre>`;
  } catch (e) { $("dy-result").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

// ---- Write tab: CQL ----
async function runCql() {
  const query = $("cql-query").value;
  const ks = $("cql-ks").value.trim();
  if (!query.trim()) { $("cql-result").innerHTML = `<div class="empty">enter a statement</div>`; return; }
  try {
    const { status, body } = await postJSON(SEED, "/admin/data/cql", { query, keyspace: ks || null });
    if (status !== 200 || !body.results) {
      $("cql-result").innerHTML = `<div class="err-line">${esc((body && body.error) || ("HTTP " + status))}</div>`;
      return;
    }
    $("cql-result").innerHTML = body.results.map(renderCqlResult).join("");
  } catch (e) { $("cql-result").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
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

// ---- Write tab: bulk seed ----
let seeding = false;
// This panel's own table selection (its `#seed-table` dropdown), independent
// of the Dynamo op panel's `dyTable` — each write-tab panel owns its own now.
let seedTable = "";

// The seed target must be a table that *has a tablet* — the exact set
// `/admin/data/seed` accepts (it validates against the replicated tablet map),
// so both Dynamo and CQL `ks.table` tables qualify once provisioned. Refreshed
// by render() (which also runs mid-seed via loadAll), so keep the Seed
// button's disabled state in sync with both table validity and `seeding`.
// Auto-picks the first seedable table when none is selected yet or the
// current selection no longer qualifies.
function renderSeedTables() {
  const tablets = (STATE.status && STATE.status.tablets) || {};
  const seedable = [...new Set(Object.values(tablets).map((t) => t.table).filter(Boolean))].sort();
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
    : "no table has a tablet yet — write to one once (Dynamo/CQL panel), then it can be seeded";
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
  // Refresh the Nodes/Tablets view live so splits appear during a large seed —
  // but NEVER block a seed chunk on it. loadAll() serializes a full cluster-wide
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
        `<div class="muted">seeded ${done.toLocaleString()} / ${total.toLocaleString()} keys · ${rate.toLocaleString()}/s`
        + (body.error ? ` · <span class="err-line">last error: ${esc(body.error)}</span>` : "") + `</div>`;
      if (wrote === 0) break; // persistent failure — don't spin
      liveRefresh(); // non-blocking, throttled — splits show live without gating throughput
    }
    await loadAll(); // final authoritative refresh once seeding settles
    if (done >= total) $("seed-status").innerHTML += `<div>${pill("ok", "done — " + done.toLocaleString() + " keys")}</div>`;
    else if (!seeding) $("seed-status").innerHTML += `<div class="muted">stopped at ${done.toLocaleString()}</div>`;
  } catch (e) {
    $("seed-status").innerHTML = `<div class="err-line">${esc(e)}</div>`;
  } finally {
    seeding = false;
    renderSeedTables(); // recompute seed-go (stays disabled if no tables remain)
    $("seed-stop").disabled = true;
  }
}
