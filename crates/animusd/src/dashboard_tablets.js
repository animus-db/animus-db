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
//
// Split lineage/placing panel (docs/roadmap.md U-05): a second, sibling
// detail card, `#tb-lineage`, keyed by the same `tbSelectedId` as the Raft/
// storage detail card above. It reads `GET /admin/system-table?kind=
// split_lineage` (ADR 0050 fork F9) and `?kind=split_placing` (ADR 0062 §2)
// — the control plane's own replicated provenance, not anything derived
// from `status.tablets` (a retired split parent has no tablet-map row left
// at all; its lineage only lives here). Neither kind can be filtered by
// tablet id server-side (the route's only filters are `kind`/`after`/
// `limit`, per its own doc in `admin.rs`), and finding a tablet's ancestors
// needs a parent-id point lookup while finding its children needs the
// REVERSE lookup (which row's `parent` equals this tablet) — no single
// point read answers either, so `loadTabletLineage` fetches the whole kind
// (paginating via `next_after`) and builds both directions client-side,
// capped at `LINEAGE_FETCH_PAGE_CAP` pages as a safety bound (see that
// constant's own doc). Fetched from `SEED` (the node this console is
// attached to): `split_lineage`/`split_placing` are ordinary replicated
// `Metadata` collections mirrored identically on every control-role node's
// own system keyspace (ADR 0038), the same "any control-role node answers
// alike, no per-node fan-out" reasoning `controlMembers`
// (`dashboard_core.js`) already documents — and the Tablets tab itself is
// only ever shown on a control-role node (`ROLE_TABS`), so this never hits
// a data-only node's `{"available": false}` in practice (handled anyway).
// Refetched on selection change AND on this tab's existing `loadAll()` poll
// cadence (`renderTablets()` runs on every tick) — no dedicated timer.
// Depends on `dashboard_core.js` (STATE, $, esc, pill, dot, idSpan, getJSON,
// SEED, humanBytes, nodeIdOf, cpGroupsByTablet, autoSplitThresholds,
// tabletStatus, tokenBound, gotoStorage, splitHiddenTable).

let tbTableFilter = "all";
let tbStatusFilter = "all";
let tbSelectedId = null;
let tbDetailStorage = null; // { tablet, data } | { tablet, error } | null
let tbLineage = null; // { tablet, ancestors, childrenOf, placing, unavailable } | { tablet, error } | null

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
  renderTabletLineage();
  // This tab's existing poll cadence (`loadAll()`'s `setInterval`, dashboard.html)
  // drives `renderTablets()` every tick regardless of which panel is open — so
  // re-fetching the lineage panel's data here, unconditionally on a selection,
  // refreshes it on that same cadence with no dedicated timer of its own.
  // Fire-and-forget: `loadTabletLineage` re-renders itself once it resolves.
  if (tbSelectedId != null) loadTabletLineage(tbSelectedId);
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
  if (tbSelectedId === id) {
    tbSelectedId = null; tbDetailStorage = null; tbLineage = null; renderTablets(); return;
  }
  tbSelectedId = id;
  tbDetailStorage = null;
  tbLineage = null;
  renderTablets();
  loadTabletDetailStorage(id);
  loadTabletLineage(id);
}

function renderTabletDetail(tablets, groups) {
  if (tbSelectedId == null || !tablets[tbSelectedId]) { $("tb-detail").style.display = "none"; return; }
  const t = tablets[tbSelectedId];
  const gs = groups[tbSelectedId] || [];
  const lead = gs.find((x) => x.g.is_leader);

  // Full per-replica Raft detail (docs/roadmap.md U-01): term, commit,
  // applied, durable, and snapshot indices, plus the log length — every
  // field `CpRaftView` (admin.rs) carries beyond the leader/role summary the
  // row itself already shows. `durable_index`/`commit_index` diverging from
  // `last_applied` is exactly the replication-lag signal ADR 0021 §3's
  // design calls for surfacing.
  const replicaRows = (t.replicas || []).map((rid) => {
    const g = gs.find((x) => nodeIdOf(x.node) === rid);
    const role = g ? (g.g.is_leader ? "leader" : "follower") : "unreachable";
    const dotCls = g ? (g.g.is_leader ? "ok-dot" : "dim-dot") : "bad-dot";
    const meta = g
      ? `t${g.g.term} · commit ${g.g.commit_index} · applied ${g.g.last_applied} · durable ${g.g.durable_index} · snapshot ${g.g.snapshot_index} · log ${g.g.log_len}${g.g.quiesced ? " · quiesced" : ""}`
      : "—";
    return `<div class="replica-row">${dot(dotCls)}${idSpan(rid, "node mono")}
      <span class="role" style="color:${role === "leader" ? "var(--accent)" : role === "unreachable" ? "var(--danger)" : "var(--text2)"}">${esc(role)}</span>
      <span class="meta">${esc(meta)}</span></div>`;
  }).join("");

  // This tablet's own live voter/learner sets (ADR 0058 Train 1) — a
  // converged group's replicas all report the same sets, so the leader's own
  // view (or, absent a reachable leader, any reachable replica's) is the
  // representative one. Deliberately NOT derived from `t.replicas`
  // (`Metadata`'s own tablet-map view of who SHOULD host this tablet) —
  // `CpRaftView.voters`/`.learners` is this replica's live Raft config, the
  // ground truth during a mid-reconfigure window where the two can disagree.
  const repView = lead || gs[0];
  const votersLearnersHtml = repView ? `<div class="meta" style="margin-top:8px">
      <div>Voters: ${(repView.g.voters || []).map((v) => idSpan(v, "mono")).join(" ") || `<span class="muted">—</span>`}</div>
      <div>Learners: ${(repView.g.learners || []).length ? repView.g.learners.map((v) => idSpan(v, "mono")).join(" ") : `<span class="muted">none</span>`}</div>
    </div>` : "";

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
    <div style="margin-bottom:18px">${replicaRows || `<div class="empty">no replicas</div>`}${votersLearnersHtml}</div>
    <h3>Storage engine</h3>
    ${storageHtml}
    <div class="row" style="margin-top:16px">
      <button id="tb-open-storage">Open in Storage →</button>
    </div>`;
  $("tb-detail").style.display = "";
  $("tb-detail-close").addEventListener("click", () => {
    tbSelectedId = null; tbDetailStorage = null; tbLineage = null; renderTablets();
  });
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

// ---- Split lineage / directed placing panel (docs/roadmap.md U-05) ----

// Safety bound on how many `/admin/system-table?kind=split_lineage`/
// `split_placing` pages `fetchSystemTableAll` will walk before giving up —
// each page is the route's own max `limit` (1000), so this caps a single
// panel load at 20,000 rows of either kind. There is no id-scoped filter on
// this route (its own doc in `admin.rs`: only `kind`/`after`/`limit`), and
// answering "what are this tablet's ancestors/children" needs to see every
// row either way (an ancestor lookup is a point read by id, but a children
// lookup is the REVERSE — which rows name this tablet as `parent` — so
// nothing short of the whole kind answers it). A real cluster's total split
// count is normally small next to this bound; if it's ever exceeded, the
// panel silently works from a partial view rather than hanging the tab on
// an unbounded fetch — a `truncated` flag would be the natural follow-up
// if that ever becomes a real limitation, not attempted here.
const LINEAGE_FETCH_PAGE_CAP = 20;

// Fetch every row of one `EntityKind` from `GET /admin/system-table`,
// walking `next_after` until the route reports no more pages or
// `LINEAGE_FETCH_PAGE_CAP` is hit. Returns `{available, rows}` — `available:
// false` mirrors the route's own honest-absence shape for a data-only node
// (`ctx.control_storage` is `None`), which the Tablets tab should never
// actually hit (`ROLE_TABS` never shows it there) but is handled rather than
// assumed away.
async function fetchSystemTableAll(base, kind) {
  const rows = [];
  let after = null;
  for (let page = 0; page < LINEAGE_FETCH_PAGE_CAP; page++) {
    let qs = "/admin/system-table?kind=" + encodeURIComponent(kind) + "&limit=1000";
    if (after) qs += "&after=" + encodeURIComponent(after);
    const r = await getJSON(base, qs);
    if (!r.available) return { available: false, rows: [] };
    rows.push(...(r.items || []));
    if (!r.truncated || !r.next_after) break;
    after = r.next_after;
  }
  return { available: true, rows };
}

// Loads (or reloads) the lineage panel's data for `id`: this tablet's
// upward ancestor chain (from `split_lineage`, walking `child -> parent`
// one hop at a time until a tablet with no lineage row of its own is
// reached — the root of the chain), its downward children (the REVERSE of
// that same map — every row whose own `parent` field equals `id`), and its
// `split_placing` row, if any. Ignores a stale response if the selection
// moved on while the fetch was in flight, the same discipline
// `loadTabletDetailStorage` uses above.
async function loadTabletLineage(id) {
  try {
    const [lineage, placing] = await Promise.all([
      fetchSystemTableAll(SEED, "split_lineage"),
      fetchSystemTableAll(SEED, "split_placing"),
    ]);
    if (tbSelectedId !== id) return;
    if (!lineage.available) { tbLineage = { tablet: id, unavailable: true }; renderTabletLineage(); return; }

    const byChild = new Map(); // child tablet id (string) -> its own split_lineage row value
    const childrenOf = new Map(); // parent tablet id (string) -> [child id, ...]
    for (const row of lineage.rows) {
      const childId = String(row.id);
      const v = row.value || {};
      byChild.set(childId, v);
      const parentId = String(v.parent);
      if (!childrenOf.has(parentId)) childrenOf.set(parentId, []);
      childrenOf.get(parentId).push(childId);
    }
    const placingByTablet = new Map();
    for (const row of placing.rows) placingByTablet.set(String(row.id), row.value);

    // Walk upward one hop per `split_lineage` row. Bounded by the row count
    // plus one (`split_lineage` is a tree keyed child -> parent, written
    // once per cutover — a cycle should never occur, but this walks
    // client-rendered data from a live system, so bound it defensively
    // rather than trust an `Array`-shaped `while (true)`).
    const ancestors = [];
    let cur = String(id);
    const seenAncestors = new Set([cur]);
    for (let i = 0; i <= lineage.rows.length; i++) {
      const entry = byChild.get(cur);
      if (!entry) break;
      const parentId = String(entry.parent);
      ancestors.push({
        parent: parentId,
        child: cur,
        cutover_wall_ms: entry.cutover_wall_ms,
        parents_final_epoch: entry.parents_final_epoch,
      });
      if (seenAncestors.has(parentId)) break;
      seenAncestors.add(parentId);
      cur = parentId;
    }

    tbLineage = { tablet: id, ancestors, childrenOf, placing: placingByTablet.get(String(id)) || null };
  } catch (e) {
    if (tbSelectedId !== id) return;
    tbLineage = { tablet: id, error: String(e) };
  }
  renderTabletLineage();
}

// Renders the descendant subtree rooted at `id` (NOT including `id` itself)
// as a nested list — every generation `childrenOf` records, however many
// splits deep. Returns "" for a childless tablet so a caller can fall back
// to an empty-state message.
function renderLineageDescendants(id, childrenOf) {
  const kids = childrenOf.get(String(id)) || [];
  if (!kids.length) return "";
  return `<ul class="lineage-tree">` + kids.map((k) =>
    `<li>${idSpan(k, "mono")}${renderLineageDescendants(k, childrenOf)}</li>`
  ).join("") + `</ul>`;
}

function renderTabletLineage() {
  const el = $("tb-lineage");
  if (tbSelectedId == null) { el.style.display = "none"; return; }
  el.style.display = "";

  if (!tbLineage || tbLineage.tablet !== tbSelectedId) {
    el.innerHTML = `<h3>Split lineage</h3><div class="empty">loading…</div>`;
    return;
  }
  if (tbLineage.error) {
    el.innerHTML = `<h3>Split lineage</h3><div class="err-line">${esc(tbLineage.error)}</div>`;
    return;
  }
  if (tbLineage.unavailable) {
    el.innerHTML = `<h3>Split lineage</h3><div class="empty">no control-plane system keyspace reachable</div>`;
    return;
  }

  // Ancestry, nearest first: this tablet's immediate parent, then that
  // parent's own parent, and so on as far as `split_lineage` goes.
  const ancestorsHtml = tbLineage.ancestors.length
    ? tbLineage.ancestors.map((a) => `<div class="replica-row">
        ${idSpan(a.parent, "mono")}<span class="muted">→</span>${idSpan(a.child, "mono")}
        <span class="meta">cutover ${esc(a.cutover_wall_ms != null ? new Date(a.cutover_wall_ms).toLocaleString() : "—")}${
          a.parents_final_epoch != null ? `, parent's final stream epoch ${esc(a.parents_final_epoch)}` : ""
        }</span>
      </div>`).join("")
    : `<div class="empty">no lineage (never split)</div>`;

  const childrenTree = renderLineageDescendants(tbSelectedId, tbLineage.childrenOf);
  const childrenHtml = childrenTree || `<div class="empty">no children</div>`;

  const p = tbLineage.placing;
  let placingHtml;
  if (!p) {
    placingHtml = `<div class="empty">no pending placing</div>`;
  } else {
    const target = p.target && p.target.length
      ? p.target.map((n) => idSpan(n, "mono")).join(" ")
      : `<span class="muted">unsatisfiable at cutover</span>`;
    placingHtml = `<div class="replica-row">
      <span class="meta">target:</span> ${target}
      ${pill(p.done ? "healthy" : "forming", p.done ? "done" : "pending")}
    </div>`;
  }

  el.innerHTML = `
    <h3>Ancestry</h3>
    <div style="margin-bottom:18px">${ancestorsHtml}</div>
    <h3>Children</h3>
    <div style="margin-bottom:18px">${childrenHtml}</div>
    <h3>Directed placing</h3>
    ${placingHtml}`;
}
