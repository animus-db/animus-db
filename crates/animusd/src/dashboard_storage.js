"use strict";
// The Storage view: folded-in debug tools that don't fit the console's other
// views — per-tablet-per-node WAL/LSM inspection and a raw key
// browser/inspector. Ported from the pre-redesign dashboard (unchanged
// behavior, restyled), since the animusd admin design doesn't include this
// level of manual storage debugging at all and it would otherwise be lost.
// (The bulk-seed tool used to live here too; it writes real DynamoDB items
// now, so it moved to the Data Browser's DynamoDB panel,
// `dashboard_browser.js`.) Depends on `dashboard_core.js` (STATE, $, esc,
// pill, getJSON, bytes, humanBytes, nodeIdOf, syncStorageUrl,
// applyPendingStorageParams, pendingStorageParams) and
// `dashboard_streams.js` (monoDuration — the TTL reaper card's own
// `last_tick_at_ms` renderer; see that function's own doc for why an
// `env.now()`-derived value can only ever render as a relative duration,
// never an absolute time).
//
// docs/roadmap.md U-07: a read-only "TTL reaper" card fed from each node's
// own `GET /admin/ttl` (`STATE.nodes[*].ttl`, `dashboard_core.js`'s
// existing per-node loadAll() fan-out — unlike `/admin/backup-store`
// (control-leader-only, one SEED fetch), the TTL reaper runs on EVERY
// node, self-gated per tablet, so this card is genuinely per-node, not a
// single shared answer).
//
// docs/roadmap.md U-07's third route: a read-only "GC (stream segment
// janitor)" card beside it, fed from `STATE.gc` (`dashboard_core.js`'s
// existing single SEED-only `GET /admin/gc` fetch, `loadAll()`'s own
// `backupStore` precedent — this janitor is control-plane-leader-only,
// exactly like the backup janitor, not per-node like the TTL reaper just
// above, so it gets that route's own fetch shape instead). Placed on the
// Storage tab rather than Backups: this janitor sweeps DynamoDB Streams
// segment objects/catalog rows (`stream_shards`, `SegmentStoreHandle`) — a
// wholly different store/subsystem than the on-demand backup store the
// Backups tab's own card covers — and "storage janitor diagnostics" is
// exactly this tab's existing theme (the TTL reaper card sits here for the
// identical reason).
//
// docs/roadmap.md U-07's fourth and last route: a read-only "Segment
// store" card beside it, fed from each node's own `GET /admin/segment-store`
// (`STATE.nodes[*].segmentStore`, `dashboard_core.js`'s existing per-node
// loadAll() fan-out) — per-node like the TTL reaper card above, not
// SEED-only like the GC/backup-store cards, since this route's own
// `local_objects`/`local` fields are genuinely per-node facts (the lesson
// recorded when `/admin/ttl` landed: a card's fetch shape must match its
// route's own gating).

// docs/roadmap.md U-07: the "TTL reaper" card — every TTL-enabled table
// (from any node that answered, since the catalog is identical everywhere)
// plus one row per reachable node showing its own reaper phase, resume
// cursor, counters, and last error, straight off `GET /admin/ttl`'s
// response shape (`{reaper, tables, leader_tablets}`).
function renderTtlReaper() {
  const el = $("ttl-body");
  if (!el) return;
  const nodes = STATE.nodes.filter((n) => n.ok);
  if (!nodes.length) { el.innerHTML = `<div class="empty">unavailable</div>`; return; }
  const withTtl = nodes.find((n) => n.ttl);
  const tables = (withTtl && withTtl.ttl.tables) || [];
  const tableRows = tables.length
    ? tables.map((t) => `<div class="list-row"><span class="detail">${esc(t.name)}</span><span class="status-text mono">${esc(t.attribute)}</span></div>`).join("")
    : `<div class="list-row"><span class="detail">no TTL-enabled tables</span></div>`;
  const nodeRows = nodes.map((n) => {
    const t = n.ttl;
    if (!t) {
      return `<div class="list-row"><span class="id">${esc(n.addr)}</span><span class="muted">unavailable</span></div>`;
    }
    const r = t.reaper || {};
    const phase = r.phase || "idle";
    const cursor = r.cursor
      ? `${esc(r.cursor.table)} · tablet ${esc(r.cursor.tablet_id)}${r.cursor.key_hex ? " · " + esc(r.cursor.key_hex) : ""}`
      : "—";
    const errRow = r.last_error
      ? `<div class="list-row"><span class="detail">&nbsp;&nbsp;last error</span><span class="err-line">${esc(r.last_error)}</span></div>`
      : "";
    return `
      <div class="list-row">
        <span class="id">${esc(n.addr)}</span>
        <span>${pill(phase === "idle" ? "forming" : "ok", phase.toUpperCase())}</span>
        <span class="muted">leads ${esc(t.leader_tablets)} tablet(s)</span>
      </div>
      <div class="list-row"><span class="detail">&nbsp;&nbsp;last tick</span><span class="status-text mono">${r.last_tick_at_ms != null ? "t+" + monoDuration(r.last_tick_at_ms) : "—"}</span></div>
      <div class="list-row"><span class="detail">&nbsp;&nbsp;cursor</span><span class="status-text mono">${cursor}</span></div>
      <div class="list-row"><span class="detail">&nbsp;&nbsp;deleted (tick / total) · expired seen</span><span class="status-text mono">${esc(r.deleted_last_tick ?? 0)} / ${esc(r.deleted_total ?? 0)} · ${esc(r.expired_seen_total ?? 0)}</span></div>
      ${errRow}
    `;
  }).join("");
  el.innerHTML = tableRows + nodeRows;
}

// docs/roadmap.md U-07: the "GC" card — the segment janitor's own live
// phase and counters plus whether this node is the control leader,
// straight off `GET /admin/gc`'s response shape (`{janitor, leader}`;
// mirrors `dashboard_backups.js::renderBackupStore`'s own layout since
// both routes share the identical control-leader-only gating).
function renderGcJanitor() {
  const el = $("gc-body");
  if (!el) return;
  const gc = STATE.gc;
  if (!gc) { el.innerHTML = `<div class="empty">unavailable</div>`; return; }
  const j = gc.janitor || {};
  const phase = j.phase || "idle";
  const rows = [
    `<div class="list-row"><span class="detail">janitor</span><span>${
      pill(phase === "idle" ? "forming" : "ok", phase.replace("_", " ").toUpperCase())
    }${gc.leader ? "" : ` <span class="muted">(not control leader)</span>`}</span></div>`,
    `<div class="list-row"><span class="detail">last tick</span><span class="status-text mono">${
      j.last_tick_at_ms != null ? "t+" + monoDuration(j.last_tick_at_ms) : "—"
    }</span></div>`,
    `<div class="list-row"><span class="detail">retention</span><span class="status-text mono">${
      j.retention_ms != null ? `${esc(j.retention_ms)}ms` : "—"
    }</span></div>`,
    `<div class="list-row"><span class="detail">deleted (tick / total) · seen total</span><span class="status-text mono">${
      esc(j.deleted_last_tick ?? 0)} / ${esc(j.orphans_deleted_total ?? 0)} · ${esc(j.orphans_seen_total ?? 0)
    }</span></div>`,
    `<div class="list-row"><span class="detail">pending (awaiting retention)</span><span class="status-text mono">${esc(j.pending_orphans ?? 0)}</span></div>`,
  ];
  if (j.last_error) {
    rows.push(`<div class="list-row"><span class="detail">last error</span><span class="err-line">${esc(j.last_error)}</span></div>`);
  }
  el.innerHTML = rows.join("");
}

// docs/roadmap.md U-07's fourth and last route: the "Segment store" card
// — the shard→replica placement every node sees (identical everywhere,
// since it's a replicated catalog fact; `null`/absent for the single-
// shared-directory `fs` opt-in) plus one row per reachable node showing
// its own store kind/location and a bounded local object count/bytes,
// straight off `GET /admin/segment-store`'s response shape (`{store,
// shards, local_objects}`).
function renderSegmentStore() {
  const el = $("seg-store-body");
  if (!el) return;
  const nodes = STATE.nodes.filter((n) => n.ok);
  if (!nodes.length) { el.innerHTML = `<div class="empty">unavailable</div>`; return; }
  const withShards = nodes.find((n) => n.segmentStore && Array.isArray(n.segmentStore.shards));
  const shards = (withShards && withShards.segmentStore.shards) || [];
  const shardRows = shards.length
    ? shards.map((s) => `<div class="list-row"><span class="detail">${esc(s.shard)}</span><span class="status-text mono">${(s.replicas || []).join(", ") || "—"}</span></div>`).join("")
    : `<div class="list-row"><span class="detail">${withShards ? "no shards yet" : "no shard placement (fs store, or unavailable)"}</span></div>`;
  const nodeRows = nodes.map((n) => {
    const s = n.segmentStore;
    if (!s) {
      return `<div class="list-row"><span class="id">${esc(n.addr)}</span><span class="muted">unavailable</span></div>`;
    }
    const store = s.store;
    const lo = s.local_objects;
    return `
      <div class="list-row">
        <span class="id">${esc(n.addr)}</span>
        <span class="status-text mono">${store ? esc(store.kind) + (store.location ? " · " + esc(store.location) : "") : "—"}</span>
      </div>
      <div class="list-row"><span class="detail">&nbsp;&nbsp;local objects</span><span class="status-text mono">${
        lo ? `${esc(lo.count)}${lo.truncated ? "+" : ""} obj · ${esc(humanBytes(lo.bytes))}${lo.truncated ? " (partial)" : ""}` : "—"
      }</span></div>
    `;
  }).join("");
  el.innerHTML = shardRows + nodeRows;
}

function renderStorageSelectors() {
  renderTtlReaper();
  renderGcJanitor();
  renderSegmentStore();
  const status = STATE.status;
  const tablets = status && status.tablets ? Object.keys(status.tablets).map(Number).sort((a, b) => a - b) : [1];
  const tsel = $("st-tablet");
  const prevT = tsel.value;
  tsel.innerHTML = tablets.map((id) => `<option value="${id}">tablet ${id}</option>`).join("");
  if (prevT && [...tsel.options].some((o) => o.value === prevT)) tsel.value = prevT;
  updateStorageNodeOptions();
  updateControlStorageNodeOptions();
  renderSystemTableKindOptions();
  // A deep-linked tablet/node (from the URL on load, or a browser back/forward
  // into the Storage tab, or the Tablets view's "Open in Storage" link) is
  // applied once the options it needs actually exist.
  if (pendingStorageParams) applyPendingStorageParams();
}

// The control-plane system-keyspace storage section (ADR 0038 PR4) is scoped
// to nodes with a LOCAL control role — a control-only or combined node, never
// a data-only one (which has no local control `RaftCore`/engine at all, ADR
// 0035; its Storage tab isn't even shown, ROLE_TABS in `dashboard_core.js`).
// Independent of `updateStorageNodeOptions`'s per-tablet-hosting filter above:
// a control-only node hosts no CP tablet group, so it would never appear
// there even though it's exactly the node this section exists to surface.
function updateControlStorageNodeOptions() {
  const sel = $("ctl-node");
  const prev = sel.value;
  const nodes = STATE.nodes.filter((n) => n.ok && (n.role === "control" || n.role === "combined"));
  sel.innerHTML = nodes.map((n) =>
    `<option value="${esc(n.base)}">node ${esc(n.addr)} (${esc(n.role)})</option>`).join("");
  if (prev && [...sel.options].some((o) => o.value === prev)) sel.value = prev;
  $("ctl-hint").textContent = nodes.length ? "" : "no reachable control-role node";
}

// The system-keyspace BROWSE section (plan-syskv-ui, an ADR 0038 addendum) —
// nested in the same "Control system keyspace" card, reusing `ctl-node`'s
// control-role-only node selector so it never offers a node with no local
// control engine at all. The kind filter lists EVERY `EntityKind`
// (`syskv.rs`), including the internal/legacy/lower-visibility ones
// (Counter/CpMemberAddr) — full transparency by the project owner's own
// call, labeled rather than hidden, since hiding them would make "what does
// this node actually store" a lie by omission. (A third such kind,
// NodeIdAlloc — the ADR 0036 allocator's idempotency ledger — was removed in
// ADR 0040 PR4 along with the allocator itself.) docs/roadmap.md U-01:
// extended from the original 7 real kinds (plus a stray "keyspace" entry
// that never matched any real `EntityKind::from_segment` segment and so
// could never return a row — dropped here, not carried forward) to all 16
// `EntityKind` variants as of this list's own writing; each value is that
// variant's own `as_str()` segment — keep this list and that `match` in sync
// by hand (this crate has no `EntityKind::ALL`/iterator to derive it from).
const SYSTEM_TABLE_KINDS = [
  ["", "(all kinds)"],
  ["tablet", "tablet"],
  ["member", "member"],
  ["schema", "schema"],
  ["policy", "policy"],
  ["node_addrs", "node_addrs"],
  ["counter", "counter (internal)"],
  ["cp_member_addr", "cp_member_addr (legacy)"],
  ["stream_shard", "stream_shard"],
  ["index_backfill", "index_backfill"],
  ["split_lineage", "split_lineage"],
  ["split_placing", "split_placing"],
  ["backup", "backup"],
  ["backup_progress", "backup_progress"],
  ["restore", "restore"],
  ["pitr_segment", "pitr_segment"],
  ["pitr_base_backup", "pitr_base_backup"],
];

// The forward-only pager's cursor for the CURRENTLY DISPLAYED page — `null`
// means "first page" (or "no further page"). `GET /admin/system-table`'s
// pagination is exclusive-after (ADR 0038 addendum), so there is no "previous
// page" without re-walking from the start — matching the plan's deliberately
// simple forward-only pager (this is a debug/inspection tool, not a general
// data browser).
let systemTableAfter = null;

function renderSystemTableKindOptions() {
  const sel = $("ctl-kind");
  if (sel.options.length) return; // a fixed list — populate once, not per-refresh
  sel.innerHTML = SYSTEM_TABLE_KINDS.map(([value, label]) =>
    `<option value="${esc(value)}">${esc(label)}</option>`).join("");
}

async function loadSystemTable(reset) {
  const base = $("ctl-node").value;
  if (!base) { $("ctl-browse-body").innerHTML = `<div class="empty">pick a control node</div>`; return; }
  if (reset) systemTableAfter = null;
  const kind = $("ctl-kind").value;
  let qs = "/admin/system-table?limit=50";
  if (kind) qs += "&kind=" + encodeURIComponent(kind);
  if (systemTableAfter) qs += "&after=" + encodeURIComponent(systemTableAfter);
  try {
    const r = await getJSON(base, qs);
    if (!r.available) {
      $("ctl-applied-index").textContent = "";
      $("ctl-next-page").disabled = true;
      $("ctl-browse-body").innerHTML = `<div class="empty">no control-plane system-keyspace engine on this node</div>`;
      return;
    }
    $("ctl-applied-index").textContent = "as of index " + r.applied_index;
    const rows = (r.items || []).map((it) => {
      const full = JSON.stringify(it.value, null, 2);
      const preview = full.length > 60 ? full.slice(0, 60).replace(/\n/g, " ") + "…" : full.replace(/\n/g, " ");
      return `<tr>
        <td class="mono">${esc(it.kind)}</td>
        <td class="mono">${esc(it.id)}</td>
        <td class="mono">${esc(it.version)}</td>
        <td class="mono"><details><summary>${esc(preview)}</summary><pre>${esc(full)}</pre></details></td>
      </tr>`;
    }).join("");
    const more = r.truncated
      ? `<div class="muted">showing ${esc(r.count)} (truncated at limit ${esc(r.limit)}) — Next page for more</div>`
      : `<div class="muted">${esc(r.count)} row(s)${kind ? " of kind " + esc(kind) : ""}</div>`;
    $("ctl-browse-body").innerHTML = rows
      ? more + `<table><thead><tr><th>kind</th><th>id</th><th>version</th><th>value</th></tr></thead><tbody>${rows}</tbody></table>`
      : `<div class="empty">no rows${kind ? " for kind " + esc(kind) : ""}</div>`;
    systemTableAfter = r.truncated ? r.next_after : null;
    $("ctl-next-page").disabled = !r.truncated;
  } catch (e) { $("ctl-browse-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function loadControlStorage() {
  const base = $("ctl-node").value;
  if (!base) { $("ctl-storage-body").innerHTML = `<div class="empty">pick a control node</div>`; return; }
  try {
    const r = await getJSON(base, "/admin/storage/control");
    if (!r.available) {
      $("ctl-storage-body").innerHTML = `<div class="empty">no control-plane system-keyspace engine on this node</div>`;
      return;
    }
    if (r.backend === "memory" || r.sstables == null) {
      $("ctl-storage-body").innerHTML = `<div class="muted">backend memory (--ephemeral) — no WAL/SSTables; metadata does not survive a restart</div>`;
      return;
    }
    const levels = (r.levels || []).map((x) => `L${x.level}:${x.tables}`).join("  ") || "—";
    const tbl = r.sstables.map((s) => `<tr>
      <td class="mono">${esc(s.seq)}</td><td class="mono">${esc(s.level)}</td>
      <td class="mono">${esc(bytes(s.min_key))} → ${esc(bytes(s.max_key))}</td>
      <td class="mono">${esc(s.min_version)}–${esc(s.max_version)}</td>
      <td class="mono">${esc(s.file_size)}</td><td>${s.has_bloom ? "✓" : ""}</td></tr>`).join("");
    $("ctl-storage-body").innerHTML =
      `<div class="muted">backend ${esc(r.backend)} · levels ${esc(levels)}
        · memtable ${esc(r.memtable.keys)} keys / ${esc(r.memtable.approx_bytes)} B
        · WAL durable_seq ${esc(r.wal.durable_seq)} · rotations ${esc(r.wal.rotations)}
        · ${esc((r.wal.segments || []).length)} segment(s)</div>`
      + (tbl ? `<table><thead><tr><th>seq</th><th>level</th><th>key range</th><th>versions</th><th>bytes</th><th>bloom</th></tr></thead><tbody>${tbl}</tbody></table>`
             : `<div class="empty">no sstables (all in memtable)</div>`);
  } catch (e) { $("ctl-storage-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

// The storage endpoints (WAL/LSM/scan/key) are node-local — a node that hosts no
// replica of the tablet answers 404 — so offer only nodes whose /admin/raftkv view
// lists the selected tablet: the same registry the storage routes resolve
// (`local_cp`), so an offered node cannot 404. If none is reachable yet (a freshly
// provisioned or split tablet whose group is still forming), the dropdown is empty
// with a hint; the Load/Browse/inspect handlers already no-op on an empty node.
function updateStorageNodeOptions() {
  const tablet = Number($("st-tablet").value);
  const nsel = $("st-node");
  const prevN = nsel.value;
  const hostGroup = (n) => n.raftkv && (n.raftkv.groups || []).find((g) => g.tablet === tablet);
  const hosting = STATE.nodes.filter((n) => n.ok).filter(hostGroup);
  nsel.innerHTML = hosting.map((n) => {
    const tag = hostGroup(n).is_leader ? " · leader" : "";
    return `<option value="${esc(n.base)}">node ${esc(nodeIdOf(n))} (${esc(n.addr)})${tag}</option>`;
  }).join("");
  if (prevN && [...nsel.options].some((o) => o.value === prevN)) nsel.value = prevN;
  $("st-hint").textContent = hosting.length ? ""
    : "no reachable node hosts this tablet yet (group still forming?)";
}

async function loadStorage() {
  const tablet = $("st-tablet").value;
  const base = $("st-node").value;
  if (!base) return;
  $("wal-records-card").style.display = "none";
  // WAL
  try {
    const w = await getJSON(base, "/admin/storage/wal?tablet=" + tablet);
    if (w.backend === "memory" || w.segments == null) {
      $("wal-body").innerHTML = `<div class="empty">memory backend — no WAL</div>`;
    } else {
      const segs = w.segments.map((s) => `<tr>
        <td class="mono"><a href="#" data-seg="${esc(s.segment)}" class="seglink">${esc(s.segment)}</a></td>
        <td class="mono">${esc(s.bytes)}</td></tr>`).join("");
      $("wal-body").innerHTML = `<div class="muted">durable_seq ${esc(w.durable_seq)} · rotations ${esc(w.rotations)}</div>
        <table><thead><tr><th>segment</th><th>bytes</th></tr></thead><tbody>${segs}</tbody></table>`;
      document.querySelectorAll(".seglink").forEach((a) =>
        a.addEventListener("click", (e) => { e.preventDefault(); loadWalSegment(base, tablet, a.dataset.seg); }));
    }
  } catch (e) { $("wal-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
  // LSM
  try {
    const l = await getJSON(base, "/admin/storage/lsm?tablet=" + tablet);
    if (l.backend === "memory" || l.sstables == null) {
      $("lsm-body").innerHTML = `<div class="empty">memory backend — no SSTables</div>`;
    } else {
      const levels = (l.levels || []).map((x) => `L${x.level}:${x.tables}`).join("  ") || "—";
      const tbl = l.sstables.map((s) => `<tr>
        <td class="mono">${esc(s.seq)}</td><td class="mono">${esc(s.level)}</td>
        <td class="mono">${esc(bytes(s.min_key))} → ${esc(bytes(s.max_key))}</td>
        <td class="mono">${esc(s.min_version)}–${esc(s.max_version)}</td>
        <td class="mono">${esc(s.file_size)}</td><td>${s.has_bloom ? "✓" : ""}</td></tr>`).join("");
      $("lsm-body").innerHTML = `<div class="muted">levels ${esc(levels)} · memtable ${esc(l.memtable.keys)} keys / ${esc(l.memtable.approx_bytes)} B</div>`
        + (tbl ? `<table><thead><tr><th>seq</th><th>level</th><th>key range</th><th>versions</th><th>bytes</th><th>bloom</th></tr></thead><tbody>${tbl}</tbody></table>`
               : `<div class="empty">no sstables (all in memtable)</div>`);
    }
  } catch (e) { $("lsm-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function loadWalSegment(base, tablet, seg) {
  $("wal-seg").textContent = seg;
  $("wal-records-card").style.display = "";
  $("wal-records").innerHTML = `<div class="empty">loading…</div>`;
  try {
    const r = await getJSON(base, "/admin/storage/wal/segment?tablet=" + tablet + "&seg=" + seg);
    const recs = (r.records || []).map((x) => `<tr>
      <td>${esc(x.type)}</td><td class="mono">${esc(x.key ?? "")}</td>
      <td class="mono">${esc(x.version ?? "")}</td>
      <td class="mono">${esc(x.value_len ?? x.keys ?? x.ops ?? "")}</td></tr>`).join("");
    $("wal-records").innerHTML = recs
      ? `<table><thead><tr><th>type</th><th>key</th><th>version</th><th>len/keys/ops</th></tr></thead><tbody>${recs}</tbody></table>`
      : `<div class="empty">empty segment</div>`;
  } catch (e) { $("wal-records").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function inspectKey() {
  const tablet = $("st-tablet").value;
  const base = $("st-node").value;
  const key = $("key-input").value;
  if (!base || !key) { $("key-body").innerHTML = `<div class="empty">enter a key</div>`; return; }
  try {
    const k = await getJSON(base, "/admin/storage/key?tablet=" + tablet + "&key=" + encodeURIComponent(key));
    const disk = (k.disk_versions || []).map((d) =>
      `<tr><td class="mono">${esc(d.version)}</td><td>${d.tombstone ? pill("err", "tombstone") : pill("ok", "value")}</td></tr>`).join("");
    $("key-body").innerHTML = `<div class="muted">key <code>${esc(k.key)}</code> · live: ${k.live == null ? "<span class='muted'>absent</span>" : `<code>${esc(k.live)}</code>`}</div>`
      + (disk ? `<table><thead><tr><th>version</th><th>kind</th></tr></thead><tbody>${disk}</tbody></table>`
              : `<div class="empty">no on-disk versions</div>`);
  } catch (e) { $("key-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function browseKeys() {
  const tablet = $("st-tablet").value;
  const base = $("st-node").value;
  if (!base) { $("scan-body").innerHTML = `<div class="empty">pick a node</div>`; return; }
  const start = $("scan-start").value;
  const limit = $("scan-limit").value || "50";
  const qs = "/admin/storage/scan?tablet=" + tablet
    + "&start=" + encodeURIComponent(start) + "&limit=" + encodeURIComponent(limit);
  try {
    const r = await getJSON(base, qs);
    if (r.backend === "memory" && r.count === 0) {
      $("scan-body").innerHTML = `<div class="empty">no live keys (memory backend starts empty)</div>`;
      return;
    }
    const rows = (r.items || []).map((it) =>
      `<tr><td class="mono"><a href="#" class="keylink" data-key="${esc(it.key)}">${esc(it.key)}</a></td>
        <td class="mono">${esc(it.value)}</td><td class="mono">${esc(it.value_len)}</td></tr>`).join("");
    const more = r.truncated
      ? `<div class="muted">showing first ${esc(r.count)} (truncated at limit ${esc(r.limit)}); set “start ≥” past the last key to page on</div>`
      : `<div class="muted">${esc(r.count)} live key(s)</div>`;
    $("scan-body").innerHTML = rows
      ? more + `<table><thead><tr><th>key</th><th>value</th><th>bytes</th></tr></thead><tbody>${rows}</tbody></table>`
      : `<div class="empty">no live keys from “${esc(start) || "the beginning"}”</div>`;
    // Click a key to send it to the inspector below.
    document.querySelectorAll(".keylink").forEach((a) =>
      a.addEventListener("click", (e) => {
        e.preventDefault();
        $("key-input").value = a.dataset.key;
        inspectKey();
        $("key-input").scrollIntoView({ block: "nearest" });
      }));
  } catch (e) { $("scan-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}
