"use strict";
// The Streams view (ADR 0042/0043): one row per DynamoDB Stream — currently
// enabled ones (`status.schemas.tables[t].stream`) plus any DISABLED one
// still inside its retention grace window (a `status.stream_shards` row
// whose (table, label) no longer matches the table's current schema stream,
// F12-b) — with a detail panel showing the shard chain (grouped per tablet,
// each epoch row from the segment catalog) merged with the live shard
// topology from a real `DescribeStream` call, plus a live-tail poller
// (GetShardIterator → GetRecords, following NextShardIterator) and per-node
// stream metric tiles. Depends on `dashboard_core.js` (STATE, $, esc, pill,
// idSpan, humanBytes, getJSON, postJSON, SEED, loadAll, ROLE, nodeDisplayId).
//
// `viewTypeLabel`/`streamArn` are also called from `dashboard_browser.js`'s
// Data Browser Streams row (the enable/disable UI, which lives there per
// ADR 0021 — a table's stream toggle is a table-panel action, not a
// Streams-tab one), which is why this file loads before it.
//
// **Shown on every role now, including control-only** (`ROLE_TABS`,
// `dashboard_core.js`) — a control-only node holds the full replicated
// `Metadata`, so the stream list (`streamsList()`) and the shard-chain
// detail (`loadStreamDescribe`/`renderShardChain`, both pure functions of
// `Metadata`/a `DescribeStream` call) render truthfully there. Only the
// live-tail poller degrades on a control-only node — see
// `findDataPlaneNode`/`renderTailControls`'s own doc below for exactly why
// and how it degrades (verified against a live split cluster, not assumed).
// The per-node metric tiles (`renderStreamTiles`) already fan out over every
// reachable node via `loadAll()`'s existing `STATE.nodes` — unaffected by
// which node's own console is currently loaded.

// ---- shared helpers (also used by dashboard_browser.js) ----

// `StreamViewType` rides the wire in two different casings depending on
// source: the raw `#[derive(Serialize)]` enum (`status.schemas.tables[t]
// .stream.view_type` / a `status.stream_shards[]` row's `view_type`) uses
// its plain Rust variant name ("NewAndOldImages", …, animus-control's
// `schema.rs`); a DynamoDB Streams wire response (`DescribeStream`'s
// `StreamViewType`) uses AWS's own SCREAMING_SNAKE_CASE
// (`stream_view_type_str`, animus-dynamo's `wire.rs`). Normalize both to the
// AWS spelling for display, since that's the form an operator who knows
// DynamoDB will recognize.
function viewTypeLabel(vt) {
  const map = {
    NewAndOldImages: "NEW_AND_OLD_IMAGES", NewImage: "NEW_IMAGE",
    OldImage: "OLD_IMAGE", KeysOnly: "KEYS_ONLY",
    NEW_AND_OLD_IMAGES: "NEW_AND_OLD_IMAGES", NEW_IMAGE: "NEW_IMAGE",
    OLD_IMAGE: "OLD_IMAGE", KEYS_ONLY: "KEYS_ONLY",
  };
  return map[vt] || (vt ? String(vt) : "—");
}

// This adapter's synthetic ARN (`animus_dynamo::wire::stream_arn` /
// ADR 0042 §4) — built client-side rather than fetched, since the format is
// fixed and documented.
function streamArn(table, label) {
  return `arn:aws:dynamodb:animus:0:table/${table}/stream/${label}`;
}

// The inverse of `animus_cp_data::segment::shard_id` (`shardId-<tablet>-
// <epoch>`, ADR 0042 §2) — duplicated client-side the same way
// `animus_dynamo::streams_wire::parse_shard_id` duplicates it server-side
// rather than depending on a data-plane crate.
function parseShardId(shardId) {
  const m = /^shardId-(\d+)-(\d+)$/.exec(shardId || "");
  return m ? { tablet: Number(m[1]), epoch: Number(m[2]) } : null;
}

// `seal_wall_ms` (a `StreamShardRow`'s seal timestamp) is `ProdEnv::now()` —
// nanoseconds-as-ms elapsed since THAT NODE'S OWN PROCESS START, never a
// wall-clock/epoch value (see `animusd::mint_stream_label`'s own doc for the
// identical caveat on a stream's label). So this can only ever be rendered
// as a relative duration ("t+12.3s into that node's uptime"), never an
// absolute "N minutes ago" — there is no shared origin to subtract against
// across nodes/processes/restarts.
function monoDuration(ms) {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = s / 60;
  if (m < 60) return `${m.toFixed(1)}m`;
  return `${(m / 60).toFixed(1)}h`;
}

// Every tablet id currently mapped to `table` (live topology, for counting
// open shards on an ENABLED stream — one per `tablets_for_table` entry,
// mirroring `dynamo_streams::current_open_epoch`'s own enumeration).
function tabletsForTable(table) {
  const tablets = (STATE.status && STATE.status.tablets) || {};
  return Object.keys(tablets).filter((id) => tablets[id].table === table).map(Number);
}

// The segment catalog (`status.stream_shards`, ADR 0042 §3/ADR 0043 §A8),
// indexed by its own identity — `(tablet, epoch)`, never `(table, label,
// tablet, epoch)`, matching `Metadata::stream_shards`'s own keying (a
// tablet's epoch counter is a property of its physical seal history, not of
// any one stream generation).
function catalogRowsByTabletEpoch() {
  const rows = (STATE.status && STATE.status.stream_shards) || [];
  const map = new Map();
  for (const r of rows) map.set(`${r.tablet}:${r.epoch}`, r);
  return map;
}

// Every stream this cluster currently knows about: one entry per currently
// ENABLED `(table, label)` (from the replicated schema catalog) plus one per
// DISABLED-but-unreaped `(table, label)` still named by at least one
// unexpired catalog row (F12-b's grace window, ADR 0042 §11) — ground-truth
// only, nothing fabricated. `key` is `JSON.stringify([table, label])` —
// this view's own stable row/selection identity, safe against a table or
// label containing any character at all (unlike a plain string join).
function streamsList() {
  const tables = (STATE.status && STATE.status.schemas && STATE.status.schemas.tables) || {};
  const shardRows = (STATE.status && STATE.status.stream_shards) || [];
  const byKey = new Map();
  for (const r of shardRows) {
    const key = JSON.stringify([r.table, r.label]);
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(r);
  }
  const out = [];
  const seen = new Set();
  for (const tname of Object.keys(tables)) {
    const schema = tables[tname];
    if (!schema.stream) continue;
    const key = JSON.stringify([tname, schema.stream.label]);
    seen.add(key);
    out.push({
      key, table: tname, label: schema.stream.label, viewType: schema.stream.view_type,
      enabled: true, rows: byKey.get(key) || [],
    });
  }
  for (const [key, rows] of byKey) {
    if (seen.has(key)) continue;
    const [table, label] = JSON.parse(key);
    out.push({ key, table, label, viewType: rows[0].view_type, enabled: false, rows });
  }
  out.sort((a, b) => a.table.localeCompare(b.table) || a.label.localeCompare(b.label));
  return out;
}

// ---- per-node stream metric tiles (ADR 0021's first `/admin/metrics`
// consumer — see dashboard_core.js's `loadAll()`) ----
//
// `stream_segments_live`/`stream_repair_backlog` are levels the segment
// janitor (a control-plane-**leader**-only loop, animusd/CLAUDE.md's
// `segment_janitor.rs`) recomputes every tick — meaningful only on whoever
// currently believes it is the control leader; every other node's sink just
// holds its own last-observed value from whenever it last led, so those two
// are read off the control leader specifically, not summed/maxed across the
// fleet. `stream_hot_bytes`/`stream_seal_backlog_ms` are per-node,
// per-most-recently-evaluated-tablet levels (the sealer arm, `index_drain.rs`)
// — `max` across nodes is the most useful single number (the worst tablet's
// own backlog), not a true cluster-wide sum. `stream_seals_total`/
// `stream_seal_failures_total`/`stream_segments_expired_total`/
// `stream_repairs_total` are genuine per-node counters, so `sum` is exact.
// `change_log_trim_blocked` is a per-node OR-across-this-node's-own-led-
// tablets level; a fleet-wide "is anything blocked" is the OR of those.
function metricSum(nodes, name) {
  return nodes.reduce((s, n) => s + ((n.metrics && n.metrics.counters && n.metrics.counters[name]) || 0), 0);
}
function metricMax(nodes, name) {
  return nodes.reduce((m, n) => Math.max(m, (n.metrics && n.metrics.counters && n.metrics.counters[name]) || 0), 0);
}

function renderStreamTiles() {
  const nodes = STATE.nodes.filter((n) => n.ok);
  const leader = STATE.nodes.find((n) => n.ok && n.raft && n.raft.is_leader);
  const segmentsLive = leader && leader.metrics ? leader.metrics.counters.stream_segments_live : null;
  const repairBacklog = leader && leader.metrics ? leader.metrics.counters.stream_repair_backlog : null;
  const hotBytes = metricMax(nodes, "stream_hot_bytes");
  const sealBacklogMs = metricMax(nodes, "stream_seal_backlog_ms");
  const sealsTotal = metricSum(nodes, "stream_seals_total");
  const sealFailuresTotal = metricSum(nodes, "stream_seal_failures_total");
  const segmentsExpiredTotal = metricSum(nodes, "stream_segments_expired_total");
  const repairsTotal = metricSum(nodes, "stream_repairs_total");
  const trimBlocked = nodes.some((n) => n.metrics && n.metrics.counters && n.metrics.counters.change_log_trim_blocked > 0);

  const tiles = [
    {
      label: "Segments live", value: segmentsLive != null ? segmentsLive.toLocaleString() : "—",
      sub: leader ? "on control leader" : "no control leader",
    },
    {
      label: "Repair backlog", value: repairBacklog != null ? repairBacklog.toLocaleString() : "—",
      sub: repairBacklog ? "under-replicated row(s)" : "none", warn: !!repairBacklog,
    },
    { label: "Hot bytes", value: humanBytes(hotBytes) || "0 B", sub: "max across nodes, most recent seal tick" },
    { label: "Seal backlog", value: `${(sealBacklogMs / 1000).toFixed(1)}s`, sub: "oldest unsealed record, max across nodes" },
  ];
  $("sm-tiles").innerHTML = tiles.map((t) =>
    `<div class="stat-tile"><div class="label">${esc(t.label)}</div>` +
    `<div class="value"${t.warn ? ' style="color:var(--warn)"' : ""}>${esc(t.value)}</div>` +
    `<div class="sub">${esc(t.sub)}</div></div>`).join("");

  const streams = streamsList();
  $("sm-summary").innerHTML = `${streams.length} stream(s)`
    + ` · ${sealsTotal.toLocaleString()} seal(s)`
    + (sealFailuresTotal ? " " + pill("warn", `${sealFailuresTotal} failure(s)`) : "")
    + ` · ${segmentsExpiredTotal.toLocaleString()} expired · ${repairsTotal.toLocaleString()} repair(s)`
    + (trimBlocked ? " " + pill("warn", "trim blocked") : "");
}

// ---- streams list ----

function renderStreamsList() {
  const streams = streamsList();
  if (!streams.length) {
    $("sm-body").innerHTML = `<div class="empty">no streams — enable one from a table's panel in the Data Browser</div>`;
    return;
  }
  const rows = streams.map((s) => {
    const openCount = s.enabled ? tabletsForTable(s.table).length : 0;
    const sealedCount = s.rows.length;
    const expiredCount = s.rows.filter((r) => r.expired).length;
    const totalRecords = s.rows.reduce((sum, r) => sum + (r.count || 0), 0);
    const newestSeal = s.rows.reduce((max, r) => Math.max(max, r.seal_wall_ms || 0), -1);
    const shardsCell = `${openCount} open / ${sealedCount} sealed`
      + (expiredCount ? ` <span class="muted">(${expiredCount} reclaiming)</span>` : "");
    return `<tr class="clickable${smSelectedKey === s.key ? " selected" : ""}" data-key="${esc(s.key)}">
      <td>${esc(s.table)}</td>
      <td class="mono">${idSpan(s.label)}</td>
      <td>${pill(s.enabled ? "ok" : "forming", s.enabled ? "ENABLED" : "DISABLED")}</td>
      <td class="mono">${esc(viewTypeLabel(s.viewType))}</td>
      <td class="mono">${shardsCell}</td>
      <td class="mono">${esc(totalRecords.toLocaleString())}</td>
      <td class="mono">${newestSeal >= 0 ? "t+" + esc(monoDuration(newestSeal)) : "—"}</td>
    </tr>`;
  }).join("");
  $("sm-body").innerHTML = `<table>
    <thead><tr><th>Table</th><th>Label</th><th>Status</th><th>View type</th><th>Shards</th><th>Sealed records</th><th>Last seal</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
  document.querySelectorAll("#sm-body tr[data-key]").forEach((tr) =>
    tr.addEventListener("click", () => selectStream(tr.dataset.key)));
}

// ---- stream detail: shard chain + live tail ----

let smSelectedKey = null;
let smDetail = null; // { key, loading } | { key, error } | { key, sd }
let smTailShardId = null;
let smTailType = "TRIM_HORIZON";
let smTailIterator = null;
let smTailRecords = [];
let smTailStatus = "";
let smTailAuto = false;
let smTailTimer = null;
const TAIL_POLL_MS = 3000;
const TAIL_RECORD_CAP = 200;

function selectStream(key) {
  if (smSelectedKey === key) { closeStreamDetail(); return; }
  smSelectedKey = key;
  smDetail = { key, loading: true };
  resetTailState();
  renderStreams();
  loadStreamDescribe(key);
}

function closeStreamDetail() {
  smSelectedKey = null;
  smDetail = null;
  resetTailState();
  renderStreams();
}

function resetTailState() {
  stopTailAuto();
  smTailShardId = null;
  smTailIterator = null;
  smTailRecords = [];
  smTailStatus = "";
}

// `DescribeStream` is paginated (`ExclusiveStartShardId`/`LastEvaluatedShardId`,
// ADR 0042 §3) — walk every page. The page cap is a defensive bound, not a
// real limit: a single tablet's own shard chain grows only on a seal tick,
// so thousands of pages would mean something is very wrong, not a slow day.
async function loadStreamDescribe(key) {
  const [table, label] = JSON.parse(key);
  const arn = streamArn(table, label);
  const shards = [];
  let start = null;
  try {
    for (let page = 0; page < 50; page++) {
      const payload = { StreamArn: arn, Limit: 200 };
      if (start) payload.ExclusiveStartShardId = start;
      const { status, body } = await postJSON(SEED, "/admin/data/dynamo", { op: "DescribeStream", payload });
      if (status >= 300) throw new Error((body && body.message) || `HTTP ${status}`);
      const sd = body.StreamDescription;
      shards.push(...(sd.Shards || []));
      if (!sd.LastEvaluatedShardId) {
        if (smSelectedKey !== key) return; // selection changed mid-flight
        smDetail = { key, sd: { ...sd, Shards: shards } };
        pickDefaultTailShard();
        renderStreams();
        return;
      }
      start = sd.LastEvaluatedShardId;
    }
  } catch (e) {
    if (smSelectedKey !== key) return;
    smDetail = { key, error: String(e) };
    renderStreams();
  }
}

// Default the live-tail shard selector to the stream's open shard (if it has
// one — only while ENABLED) so opening a stream immediately shows its hot
// tail, else the most recently sealed shard.
function pickDefaultTailShard() {
  const sd = smDetail && smDetail.sd;
  if (!sd || !sd.Shards.length) return;
  const open = sd.Shards.find((s) => s.SequenceNumberRange.EndingSequenceNumber == null);
  smTailShardId = open ? open.ShardId : sd.Shards[sd.Shards.length - 1].ShardId;
}

function renderStreamDetail() {
  const el = $("sm-detail");
  if (!smSelectedKey) { el.style.display = "none"; return; }
  el.style.display = "";
  const [table, label] = JSON.parse(smSelectedKey);
  const arn = streamArn(table, label);
  const stream = streamsList().find((s) => s.key === smSelectedKey);
  const statusPill = stream ? pill(stream.enabled ? "ok" : "forming", stream.enabled ? "ENABLED" : "DISABLED") : "";
  const vtPill = pill("forming", viewTypeLabel(stream ? stream.viewType : null));

  let chainHtml;
  if (!smDetail || smDetail.key !== smSelectedKey || smDetail.loading) {
    chainHtml = `<div class="empty">loading shard chain…</div>`;
  } else if (smDetail.error) {
    chainHtml = `<div class="err-line">${esc(smDetail.error)}</div>`;
  } else {
    chainHtml = renderShardChain(smDetail.sd);
  }

  el.innerHTML = `
    <div class="head"><span class="id">${esc(table)}</span><button class="link-text" id="sm-detail-close">Close ×</button></div>
    <div class="sub mono" title="${esc(arn)}">${esc(label)}</div>
    <div class="row" style="margin-bottom:6px">${statusPill}${vtPill}</div>
    <h3>Shard chain</h3>
    <div>${chainHtml}</div>
    <h3>Live tail</h3>
    ${renderTailControls()}`;
  $("sm-detail-close").addEventListener("click", closeStreamDetail);
  wireTailControls();
}

// One block per tablet, epochs ascending — the shard chain grouped exactly
// the way ADR 0042 §2 describes a `ParentShardId` chain (a routine seal's
// child names the same tablet's own previous epoch; a split child's epoch-0
// names the parent *tablet's* own last shard). Per ADR 0021 §7's health
// philosophy an open/unsealed shard is a NORMAL state, never a warning — it
// only ever renders neutral ("OPEN"); the only warn signals anywhere in this
// tab are the tile-level repair-backlog/trim-blocked/seal-failure ones above.
function renderShardChain(sd) {
  const catalog = catalogRowsByTabletEpoch();
  const byTablet = new Map();
  for (const s of sd.Shards || []) {
    const parsed = parseShardId(s.ShardId);
    if (!parsed) continue;
    const arr = byTablet.get(parsed.tablet) || [];
    arr.push({ ...s, ...parsed, open: s.SequenceNumberRange.EndingSequenceNumber == null });
    byTablet.set(parsed.tablet, arr);
  }
  const tabletIds = [...byTablet.keys()].sort((a, b) => a - b);
  if (!tabletIds.length) return `<div class="empty">no shards yet</div>`;
  return tabletIds.map((tid) => {
    const shards = byTablet.get(tid).sort((a, b) => a.epoch - b.epoch);
    const rows = shards.map((s) => {
      const row = catalog.get(`${tid}:${s.epoch}`);
      const statePill = s.open ? pill("forming", "OPEN") : pill("ok", "SEALED");
      const expiredPill = row && row.expired ? " " + pill("forming", "reclaiming") : "";
      const range = `${s.SequenceNumberRange.StartingSequenceNumber} → ${s.SequenceNumberRange.EndingSequenceNumber || "…"}`;
      const countText = row ? row.count.toLocaleString() : "—";
      const sealText = row ? `t+${monoDuration(row.seal_wall_ms)}` : "—";
      const replicasHtml = row && row.replicas && row.replicas.length
        ? row.replicas.map((r) => idSpan(r, "mono")).join(" ") : "";
      return `<div class="shard-row">
        <div class="row" style="gap:8px">
          <span class="mono" style="font-weight:600">epoch ${esc(s.epoch)}</span>${statePill}${expiredPill}
          <span class="spacer"></span><span class="muted mono" style="font-size:10.5px">${esc(range)}</span>
        </div>
        <div class="muted" style="font-size:11px">${esc(countText)} record(s) · sealed ${esc(sealText)}${replicasHtml ? " · replicas " + replicasHtml : ""}</div>
      </div>`;
    }).join("");
    return `<div style="margin-bottom:10px">
      <div class="muted" style="font:600 11px var(--font-ui);margin-bottom:6px">tablet ${esc(tid)}</div>
      ${rows}
    </div>`;
  }).join("");
}

function tailShardOptions(sd) {
  if (!sd) return [];
  return sd.Shards.map((s) => ({ id: s.ShardId, open: s.SequenceNumberRange.EndingSequenceNumber == null }));
}

// The live tail (`GetShardIterator`/`GetRecords`, both called against `SEED`
// — this loaded page's own admin port) needs a genuine local CP data plane
// to serve: the sealed path reads this node's own `SegmentStoreHandle`
// (`ClientCtx::data()`, which **panics**, not errors, on a control-only
// node — verified live: an empty-reply/dropped connection, not a JSON
// error), and the open path forwards to the tablet's leader via
// `resolve_cp_route`'s blind-replica fallback, which a control-only node
// (no local replica, hence no real leader hint to chase) can only ever
// guess at — verified live: a ~10s `SCHEMA_COMMIT_TIMEOUT` stall ending in
// "not the leader here." Both are a genuine backend gap, not a UI
// shortcoming — this view simply never dials either op from a control-only
// console (`ROLE`, `dashboard_core.js`) rather than surfacing either
// failure mode. The stream list + shard-chain detail above are unaffected:
// `ListStreams`/`DescribeStream` are pure functions of the replicated
// `Metadata`, so they render identically here.
function findDataPlaneNode() {
  return STATE.nodes.find((n) => {
    const role = (n.config && n.config.role) || n.role;
    return (role === "data" || role === "combined") && n.base;
  });
}

function renderTailControls() {
  if (ROLE === "control") {
    const target = findDataPlaneNode();
    const link = target
      ? `<a href="${esc(target.base)}/admin/ui/streams" target="_blank" rel="noopener" class="link-text">open node ${esc(nodeDisplayId(target))}'s console →</a>`
      : `<span class="muted">no data node reachable from here right now</span>`;
    return `<div class="empty">live tail needs a local CP data plane — a control-only
      node has none. ${link}</div>`;
  }
  const sd = smDetail && smDetail.sd;
  const opts = tailShardOptions(sd);
  const shardSel = opts.length
    ? `<select id="sm-tail-shard">${opts.map((o) =>
        `<option value="${esc(o.id)}"${o.id === smTailShardId ? " selected" : ""}>${esc(o.id)}${o.open ? " (open)" : ""}</option>`).join("")}</select>`
    : `<span class="muted">no shards yet</span>`;
  return `<div class="row" style="margin-bottom:10px">
      ${shardSel}
      <select id="sm-tail-type">
        <option value="TRIM_HORIZON"${smTailType === "TRIM_HORIZON" ? " selected" : ""}>TRIM_HORIZON</option>
        <option value="LATEST"${smTailType === "LATEST" ? " selected" : ""}>LATEST</option>
      </select>
      <button id="sm-tail-poll"${smTailShardId ? "" : " disabled"}>Poll</button>
      <label class="row" style="gap:6px"><input type="checkbox" id="sm-tail-auto"${smTailAuto ? " checked" : ""}${smTailShardId ? "" : " disabled"}> auto (${TAIL_POLL_MS / 1000}s)</label>
    </div>
    <div class="muted" id="sm-tail-status" style="margin-bottom:8px">${esc(smTailStatus)}</div>
    <div class="tail-records"><pre id="sm-tail-pre">${esc(smTailRecords.length ? JSON.stringify(smTailRecords, null, 2) : "(no records polled yet)")}</pre></div>`;
}

function wireTailControls() {
  const shardSel = $("sm-tail-shard");
  if (shardSel) {
    shardSel.addEventListener("change", () => {
      smTailShardId = shardSel.value;
      smTailIterator = null; smTailRecords = []; smTailStatus = "";
      renderStreamDetail();
    });
  }
  const typeSel = $("sm-tail-type");
  if (typeSel) {
    typeSel.addEventListener("change", () => {
      smTailType = typeSel.value;
      smTailIterator = null; // a changed iterator type needs a fresh mint
      renderStreamDetail();
    });
  }
  const pollBtn = $("sm-tail-poll");
  if (pollBtn) pollBtn.addEventListener("click", pollTail);
  const autoCb = $("sm-tail-auto");
  if (autoCb) {
    autoCb.addEventListener("change", () => {
      if (autoCb.checked) startTailAuto(); else stopTailAuto();
    });
  }
}

// Mint a fresh iterator only when this shard/type has none yet — a held
// iterator's `NextShardIterator` is what actually advances the tail; minting
// again would jump position back to `smTailType`'s starting point (ADR 0042
// §6: stateless, non-expiring tokens, so there's nothing server-side to
// refresh).
async function ensureTailIterator() {
  if (smTailIterator || !smTailShardId) return smTailIterator;
  const [table, label] = JSON.parse(smSelectedKey);
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "GetShardIterator",
    payload: { StreamArn: streamArn(table, label), ShardId: smTailShardId, ShardIteratorType: smTailType },
  });
  if (status >= 300) throw new Error((body && body.message) || `HTTP ${status}`);
  smTailIterator = body.ShardIterator;
  return smTailIterator;
}

async function pollTail() {
  if (!smTailShardId) return;
  try {
    const it = await ensureTailIterator();
    const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
      op: "GetRecords", payload: { ShardIterator: it, Limit: 100 },
    });
    if (status >= 300) throw new Error((body && body.message) || `HTTP ${status}`);
    const recs = body.Records || [];
    const now = new Date().toLocaleTimeString();
    if (recs.length) {
      smTailRecords = [...smTailRecords, ...recs].slice(-TAIL_RECORD_CAP);
      smTailStatus = `+${recs.length} record(s) at ${now}`;
    } else {
      smTailStatus = `no new records (checked ${now})`;
    }
    smTailIterator = body.NextShardIterator || null;
    if (!smTailIterator) {
      // A closed shard eventually drains to a null iterator (ADR 0042 §2) —
      // the documented "walk to its child" signal, never an error.
      smTailStatus += " · shard exhausted — select its child shard to continue";
      stopTailAuto();
    }
  } catch (e) {
    smTailStatus = String(e);
  }
  renderStreamDetail();
}

function startTailAuto() {
  smTailAuto = true;
  if (smTailTimer) clearInterval(smTailTimer);
  smTailTimer = setInterval(pollTail, TAIL_POLL_MS);
}
function stopTailAuto() {
  smTailAuto = false;
  if (smTailTimer) { clearInterval(smTailTimer); smTailTimer = null; }
}

function renderStreams() {
  renderStreamTiles();
  renderStreamsList();
  renderStreamDetail();
}
