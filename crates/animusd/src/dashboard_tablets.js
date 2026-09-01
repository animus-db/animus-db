"use strict";
// The Tablets view: a filterable list (by table, by derived status) with
// replica-role dots, current leader, ESTIMATED live key count and byte size
// (ADR 0034 — the byte column carries an over-auto-split-threshold
// indicator, since `--auto-split-bytes B` — or, for a streamed table,
// `--auto-split-change-rate` — fires a split), and status. The counts are
// estimates on purpose: this view polls every node's `/admin/raftkv` on the
// auto-refresh interval, and the exact count materializes every hosted
// tablet's rows per request — an observer that measurably slows what it
// observes (it inflated a 20,000-row split's build ~9x). The byte estimate
// is the very counter auto-split gates on, so its pill agrees with the
// trigger that will fire; `/admin/raftkv?exact=1` is the deliberate one-off
// precise answer. The key count is purely informational now (the former
// key-count auto-split trigger was removed) and blank on the memory
// backend, which has no cheap counter. Clicking a row opens a right-side detail panel: raft group members
// (from data already fetched) plus storage-engine stats fetched on demand
// from a single node (/admin/storage/lsm) only for the selected tablet's
// leader — not for every row. No election-history section: this codebase
// tracks only current Raft state, not a history of leadership transitions.
// A replica's `quiesced` flag (ADR 0044 phase-1 PR7) renders as a neutral
// "quiesced" pill (reusing the `.forming` style — informational, not a
// health/data-risk signal, ADR 0021 §7's same rule) next to the leader cell
// and in the detail panel's per-replica meta line; this view never fetches
// it specially, since `CpRaftView` already carries it in the same payload
// `key_count`/`byte_size` come from.
// Depends on `dashboard_core.js` (STATE, $, esc, pill, dot, idSpan, getJSON,
// humanBytes, nodeIdOf, cpGroupsByTablet, autoSplitThresholds,
// tabletStatus, tokenBound, gotoStorage, splitHiddenTable).

let tbTableFilter = "all";
let tbStatusFilter = "all";
let tbSelectedId = null;
let tbDetailStorage = null; // { tablet, data } | { tablet, error } | null

function renderTablets() {
  const status = STATE.status;
  const tablets = (status && status.tablets) || {};
  const ids = Object.keys(tablets).map(Number).sort((a, b) => a - b);
  const groups = cpGroupsByTablet();
  const thresholds = autoSplitThresholds();

  $("tb-count").textContent = `${ids.length} tablet(s)`;

  // The table filter groups a GSI's hidden `<base>$<index>` tablets under
  // their base table's own name (`splitHiddenTable`, dashboard_core.js) —
  // picking "orders" matches both its own tablets and `orders$by_status`'s,
  // so an operator never has to know the hidden spelling exists.
  const tableOf = (id) => {
    const name = tablets[id].table;
    if (!name) return null;
    const h = splitHiddenTable(name);
    return h ? h.base : name;
  };
  const tableNames = [...new Set(ids.map(tableOf).filter(Boolean))].sort();
  const tsel = $("tb-table-filter");
  const prevTf = tsel.value || tbTableFilter;
  tsel.innerHTML = `<option value="all">All tables</option>`
    + tableNames.map((n) => `<option${n === prevTf ? " selected" : ""}>${esc(n)}</option>`).join("");
  tbTableFilter = tableNames.includes(prevTf) ? prevTf : "all";
  tsel.value = tbTableFilter;
  $("tb-status-filter").value = tbStatusFilter;

  const rows = ids.filter((id) => {
    const t = tablets[id];
    if (tbTableFilter !== "all" && tableOf(id) !== tbTableFilter) return false;
    if (tbStatusFilter !== "all" && tabletStatus(t, groups[id] || []) !== tbStatusFilter) return false;
    return true;
  });

  const bodyRows = rows.map((id) => {
    const t = tablets[id];
    const gs = groups[id] || [];
    const lead = gs.find((x) => x.g.is_leader);
    const st = tabletStatus(t, gs);
    const keyCount = lead && lead.g.key_count != null ? lead.g.key_count : null;
    const byteSize = lead && lead.g.byte_size != null ? lead.g.byte_size : null;
    // Purely informational now — there is no key-count auto-split trigger
    // to flag against (removed; bytes and, for streamed tables,
    // change-rate are the only remaining triggers).
    const byteOver = byteSize != null && thresholds.bytes != null && byteSize > thresholds.bytes;
    const keysCell = keyCount == null
      ? `<span class="muted">—</span>`
      : esc(keyCount.toLocaleString());
    const sizeCell = byteSize == null
      ? `<span class="muted">—</span>`
      : `${esc(humanBytes(byteSize))}` + (byteOver ? " " + pill("warn", "over " + humanBytes(thresholds.bytes)) : "");
    const replicaDots = (t.replicas || []).map((rid) => {
      const g = gs.find((x) => nodeIdOf(x.node) === rid);
      const cls = g ? (g.g.is_leader ? "ok-dot" : "dim-dot") : "bad-dot";
      const title = `node ${rid}` + (g ? (g.g.is_leader ? " (leader)" : " (follower)") : " (unreachable)");
      return `<span class="dot ${cls}" title="${esc(title)}"></span>`;
    }).join("");
    return `<tr class="clickable${tbSelectedId === id ? " selected" : ""}" data-id="${esc(id)}">
      <td class="mono">${esc(id)}</td>
      <td>${tableCellHtml(t.table)}</td>
      <td class="mono">${keysCell}</td>
      <td class="mono">${sizeCell}</td>
      <td class="mono">${lead ? `node ${idSpan(nodeIdOf(lead.node))}${lead.g.quiesced ? " " + pill("forming", "quiesced") : ""}` : `<span class="muted">—</span>`}</td>
      <td><span class="replica-dots">${replicaDots}</span></td>
      <td>${pill(st, st)}</td>
    </tr>`;
  }).join("");
  $("tb-body").innerHTML = bodyRows ? `<table>
    <thead><tr><th>Tablet</th><th>Table</th><th title="Estimate, informational only. Exact: /admin/raftkv?exact=1">Keys ~</th><th title="Estimate, base-scoped (ADR 0034) \u2014 the same counter --auto-split-bytes gates on. Exact: /admin/raftkv?exact=1">Size ~</th><th>Leader</th><th>Replicas</th><th>Status</th></tr></thead>
    <tbody>${bodyRows}</tbody></table>` : `<div class="empty">no tablets match this filter</div>`;

  document.querySelectorAll("#tb-body tr[data-id]").forEach((tr) =>
    tr.addEventListener("click", () => selectTablet(Number(tr.dataset.id))));

  renderTabletDetail(tablets, groups);
}

// A hidden GSI materialization table (`orders$by_status`) renders as
// `orders › by_status` with a neutral GSI badge instead of its opaque
// `$`-joined spelling (`splitHiddenTable`, dashboard_core.js) — the
// underlying row is still a real, independently-hosted tablet; this is
// presentation only.
function tableCellHtml(name) {
  if (!name) return `<span class="muted">—</span>`;
  const h = splitHiddenTable(name);
  if (!h) return esc(name);
  return `${esc(h.base)} <span class="muted">›</span> ${esc(h.index)} ${pill("forming", "GSI")}`;
}

function selectTablet(id) {
  if (tbSelectedId === id) { tbSelectedId = null; tbDetailStorage = null; renderTablets(); return; }
  tbSelectedId = id;
  tbDetailStorage = null;
  renderTablets();
  loadTabletDetailStorage(id);
}

function renderTabletDetail(tablets, groups) {
  if (tbSelectedId == null || !tablets[tbSelectedId]) { $("tb-detail").style.display = "none"; return; }
  const t = tablets[tbSelectedId];
  const gs = groups[tbSelectedId] || [];
  const lead = gs.find((x) => x.g.is_leader);

  const replicaRows = (t.replicas || []).map((rid) => {
    const g = gs.find((x) => nodeIdOf(x.node) === rid);
    const role = g ? (g.g.is_leader ? "leader" : "follower") : "unreachable";
    const dotCls = g ? (g.g.is_leader ? "ok-dot" : "dim-dot") : "bad-dot";
    const meta = g ? `t${g.g.term} · i${g.g.last_applied}${g.g.quiesced ? " · quiesced" : ""}` : "—";
    return `<div class="replica-row">${dot(dotCls)}${idSpan(rid, "node mono")}
      <span class="role" style="color:${role === "leader" ? "var(--accent)" : role === "unreachable" ? "var(--danger)" : "var(--text2)"}">${esc(role)}</span>
      <span class="meta">${esc(meta)}</span></div>`;
  }).join("");

  let storageHtml;
  if (!lead) {
    storageHtml = `<div class="storage-grid loading">no reachable leader to query</div>`;
  } else if (!tbDetailStorage || tbDetailStorage.tablet !== tbSelectedId) {
    storageHtml = `<div class="storage-grid loading">loading…</div>`;
  } else if (tbDetailStorage.error) {
    storageHtml = `<div class="err-line">${esc(tbDetailStorage.error)}</div>`;
  } else {
    const d = tbDetailStorage.data;
    if (d.backend === "memory" || d.sstables == null) {
      storageHtml = `<div class="storage-grid loading">memory backend — no on-disk stats</div>`;
    } else {
      const totalBytes = d.sstables.reduce((sum, s) => sum + (s.file_size || 0), 0);
      storageHtml = `<div class="storage-grid">
        <div>SST files: ${esc(d.sstables.length)}</div>
        <div>Memtable: ${esc(d.memtable.keys)} keys</div>
        <div>Disk bytes: ${esc(totalBytes.toLocaleString())}</div>
        <div>Levels: ${esc((d.levels || []).map((l) => `L${l.level}:${l.tables}`).join(" ") || "—")}</div>
      </div>`;
    }
  }

  $("tb-detail").innerHTML = `
    <div class="head"><span class="id">${esc(tbSelectedId)}</span>
      <button class="link-text" id="tb-detail-close">Close ×</button></div>
    <div class="sub">${tableCellHtml(t.table)} · ${esc(tokenBound(t.range && t.range.start, "AAAAAAAAAAA"))} → ${esc(tokenBound(t.range && t.range.end, "__________8"))}</div>
    <h3>Raft group</h3>
    <div style="margin-bottom:18px">${replicaRows || `<div class="empty">no replicas</div>`}</div>
    <h3>Storage engine</h3>
    ${storageHtml}
    <div class="row" style="margin-top:16px">
      <button id="tb-open-storage">Open in Storage →</button>
    </div>`;
  $("tb-detail").style.display = "";
  $("tb-detail-close").addEventListener("click", () => { tbSelectedId = null; tbDetailStorage = null; renderTablets(); });
  $("tb-open-storage").addEventListener("click", () => gotoStorage(tbSelectedId, lead ? lead.node.base : null));
}

async function loadTabletDetailStorage(id) {
  const gs = cpGroupsByTablet()[id] || [];
  const lead = gs.find((x) => x.g.is_leader);
  if (!lead) return;
  try {
    const d = await getJSON(lead.node.base, "/admin/storage/lsm?tablet=" + id);
    if (tbSelectedId !== id) return; // selection changed while the fetch was in flight
    tbDetailStorage = { tablet: id, data: d };
  } catch (e) {
    if (tbSelectedId !== id) return;
    tbDetailStorage = { tablet: id, error: String(e) };
  }
  renderTabletDetail(STATE.status.tablets, cpGroupsByTablet());
}
