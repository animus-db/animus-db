"use strict";
// The Overview view: health banner, stat tiles, a nodes list (grouped into
// "Control plane" / "Data nodes" sections when a split deployment's
// control-only nodes exist, each reachable row linking to that node's own
// admin console), a per-table breakdown (real data — the design's "Recent
// activity" panel is dropped, since there is no backend event log to back
// it), and a tablets-per-node balance chart. Depends on `dashboard_core.js`
// having loaded first (STATE, $, esc, pill, dot, idSpan, consoleLink,
// nodeIdOf, nodeDisplayId, cpGroupsByTablet, tabletStatus, worstTabletStatus,
// statusDotClass, computeHealth, activateTab).

function renderOverview() {
  const status = STATE.status;
  const tablets = (status && status.tablets) || {};
  const tabletIds = Object.keys(tablets).map(Number).sort((a, b) => a - b);
  const groups = cpGroupsByTablet();
  const members = (status && status.members) || {};
  // ADR 0040 PR3: member ids are now self-minted strings (`"n0"`, an
  // allocator-minted `"alloc-…"`, or an arbitrary operator-proposed id), not
  // small integers — a numeric `.map(Number)` would turn every one into
  // `NaN`. Plain lexicographic sort (config/gen-config zero-pads generated
  // ids specifically so this stays == numeric order for the common case).
  const memberIds = Object.keys(members).sort();
  const nodeCount = memberIds.length || STATE.nodes.length;

  const h = computeHealth();
  // Control-only nodes are never `Metadata.members` (see the crate guide's
  // "the cluster's members are the CP raftkv nodes" entry) — they're only
  // discoverable through the `/admin/peers` fan-out. A control-only node
  // whose own `/admin/config` fetch is down still shows up here (tagged
  // "control", marked unreachable, no console link) as long as SOME peer's
  // `/admin/peers` reported its role (`n.role`, ADR 0035 residual follow-up)
  // — previously it vanished from the list entirely the moment its own
  // fan-out failed.
  const controlRows = STATE.nodes
    .filter((n) => (n.ok && n.config && n.config.role === "control") ||
      (!(n.ok && n.config) && n.role === "control"))
    .map((n) => ({
      id: (n.config && n.config.node_id != null) ? n.config.node_id : n.addr,
      role: "control", up: n.ok, base: n.ok ? n.base : null,
      detail: n.ok ? ((n.raft && n.raft.is_leader) ? "control leader" : "control node") : "control node",
      statusText: n.ok ? "reachable" : "unreachable",
    }));
  $("ov-summary").textContent = controlRows.length
    ? `${controlRows.length} control + ${nodeCount} data node(s) · ${tabletIds.length} tablet(s)`
    : `${nodeCount} node(s) · ${tabletIds.length} tablet(s)`;

  // ---- health banner ----
  // Health here means "is the data at risk", not "is anything in transition"
  // (full philosophy on `computeHealth`, dashboard_core.js). Critical is
  // either no control leader (can't accept metadata changes) or a tablet
  // below quorum (can't commit, one more failure loses data). Degraded is an
  // actual redundancy loss (under-replicated) or a formation stuck long
  // enough to be suspicious (`overdueFormingCount`) — never a routine
  // in-flight formation (split-child, first election, rebalance catch-up),
  // which shows up in the healthy banner's forming count instead, called out
  // explicitly as NOT at risk. A lingering `Down` member with nothing left
  // depending on it is also not degrading; `downCount` is still called out
  // for context. `nodeDisplayId` renders this node's one id (ADR 0040 PR1 —
  // there is no more separate raftkv/control id pair to fall back between).
  const bannerSummary = h.status === "critical"
    ? (!h.controlLeader
        ? "No control-plane leader known — the cluster cannot accept metadata changes right now."
        : `${h.quorumLostCount} tablet(s) below quorum — can't commit; another failure would lose data.`)
    : h.status === "degraded"
      ? `${h.underReplicatedCount} tablet(s) under-replicated${h.overdueFormingCount ? ` · ${h.overdueFormingCount} tablet(s) stuck forming past 60s` : ""}${h.downCount ? ` · ${h.downCount} node(s) down` : ""}.`
      : h.formingCount
        ? `All data fully replicated and not at risk · ${h.formingCount} tablet(s) provisioning · control leader ${h.controlLeader ? "node " + esc(nodeDisplayId(h.controlLeader)) : "—"}.`
        : h.downCount
          ? `${h.downCount} node(s) down, but all ${tabletIds.length} tablet(s) are fully replicated · control leader ${h.controlLeader ? "node " + esc(nodeDisplayId(h.controlLeader)) : "—"}.`
          : `All ${nodeCount} node(s) reachable · ${tabletIds.length} tablet(s) replicated · control leader ${h.controlLeader ? "node " + esc(nodeDisplayId(h.controlLeader)) : "—"}.`;
  $("ov-banner").className = "card health-banner " + h.status;
  $("ov-banner").innerHTML = `
    <div class="row" style="gap:10px">${dot((h.status === "healthy" ? "ok" : "bad") + "-dot")}
      <span style="font:600 15px var(--font-ui);color:${h.status === "healthy" ? "var(--text)" : "var(--danger)"}">${esc(h.status === "healthy" ? "Healthy" : h.status === "critical" ? "Critical" : "Degraded")}</span></div>
    <div class="muted" style="margin-top:6px">${esc(bannerSummary)}</div>`;

  // ---- stat tiles ----
  const controlTermText = h.controlLeader && h.controlLeader.raft ? `term ${h.controlLeader.raft.term}` : "—";
  // "At risk" = quorum-lost + under-replicated — the two statuses that mean
  // real redundancy loss, per `computeHealth`'s philosophy. `forming` tablets
  // are called out in the sub-text (they're a transition, not a risk) rather
  // than counted here.
  const atRiskCount = h.quorumLostCount + h.underReplicatedCount;
  const tiles = [
    { label: "Nodes", value: `${nodeCount - h.downCount}/${nodeCount}`, sub: h.downCount ? `${h.downCount} down` : "all up" },
    { label: "Tablets", value: `${tabletIds.length}`, sub: `across ${Object.keys((status && status.schemas && status.schemas.tables) || {}).length} table(s)` },
    { label: "At risk", value: `${atRiskCount}`, sub: atRiskCount ? "needs attention" : (h.formingCount ? `${h.formingCount} forming` : "none") },
    { label: "Control plane", value: h.controlLeader ? `node ${esc(nodeDisplayId(h.controlLeader))}` : "—", sub: controlTermText },
  ];
  $("ov-tiles").innerHTML = tiles.map((t) =>
    `<div class="stat-tile"><div class="label">${esc(t.label)}</div><div class="value">${t.value}</div><div class="sub">${esc(t.sub)}</div></div>`
  ).join("");

  // ---- nodes list ----
  // Every DATA member (tracked in replicated `Metadata`, keyed by raftkv id —
  // covers both a "data"-role and a "combined"-role node) plus every
  // CONTROL-ONLY node reachable via the `/admin/peers` fan-out (ADR 0035,
  // `controlRows` above): a control-only node is never a data member at all,
  // so without the second half it would never appear anywhere in the
  // dashboard despite being a real part of the cluster. In a split
  // deployment the two kinds render as separate "Control plane" / "Data
  // nodes" groups; a combined-mode cluster (no control-only nodes) keeps the
  // single flat list, each row tagged with its role either way. Every
  // reachable node's row links to that node's OWN admin console (its admin
  // `base` origin, already resolved by the fan-out) — the same
  // `target="_blank"` pattern the Node view's "Open cluster console" link
  // uses — so hopping between nodes' consoles never means retyping a port.
  // Role prefers `node.config.role` (fresher, read off that node's own
  // successful `/admin/config` fetch) and falls back to `node.role` (from
  // `/admin/peers` itself, ADR 0035 residual follow-up) so a node whose own
  // fan-out hasn't resolved yet still reads as its real role instead of a
  // generic guess.
  const dataRows = (memberIds.length ? memberIds : STATE.nodes.filter((n) => n.ok).map((n) => n.config.node_id))
    .map((id) => {
      const m = members[id];
      const node = nodeById(id);
      const up = m ? m.status === "Active" : !!(node && node.ok);
      const hostedCount = groups && Object.values(groups).flat().filter((x) => nodeIdOf(x.node) === id).length;
      const role = (node && ((node.config && node.config.role) || node.role)) || "data";
      // ADR 0040 PR6: a `Down` member that has never activated is exactly
      // the orphan-member sweep's eligibility signal (`has_activated:
      // false`) — called out here, minimally, rather than as a new column,
      // since it's the same "current status" text this row already shows.
      const neverActivated = m && m.status === "Down" && m.has_activated === false;
      return {
        id, role, up,
        base: node && node.ok ? node.base : null,
        detail: `${hostedCount} tablet(s)`,
        statusText: m
          ? (neverActivated ? "Down (never activated)" : m.status)
          : (up ? "reachable" : "unreachable"),
      };
    });
  const nodeRow = (r) => `<div class="list-row">${dot(r.up ? "ok-dot" : "bad-dot")}
      ${idSpan(r.id, "id mono")}
      <span class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.03em">${esc(r.role)}</span>
      <span class="detail">${esc(r.detail)}</span>
      ${consoleLink(r.base, r.id)}
      <span class="status-text" style="color:var(${r.up ? "--ok" : "--danger"})">${esc(r.statusText)}</span>
    </div>`;
  const groupHead = (label) =>
    `<div class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.05em;margin:10px 0 2px">${esc(label)}</div>`;
  $("ov-nodes").innerHTML = controlRows.length
    ? groupHead("Control plane") + controlRows.slice(0, 6).map(nodeRow).join("")
      + groupHead("Data nodes")
      + (dataRows.slice(0, 6).map(nodeRow).join("") || `<div class="empty">no data members yet</div>`)
    : (dataRows.slice(0, 6).map(nodeRow).join("") || `<div class="empty">no members yet</div>`);

  // ---- tables summary (real substitute for the design's activity feed) ----
  const byTable = {};
  tabletIds.forEach((id) => {
    const t = tablets[id];
    const name = t.table || "(no table)";
    (byTable[name] = byTable[name] || []).push(t);
  });
  const tableNames = Object.keys(byTable).sort();
  const tableRows = tableNames.slice(0, 6).map((name) => {
    const ts = byTable[name];
    // Worst status among the table's tablets, not "any non-healthy" — a
    // table with only `forming` tablets (e.g. right after a split) gets the
    // neutral "forming" pill, not the same orange "attention" pill as a
    // table that's actually lost redundancy.
    const worst = worstTabletStatus(ts.map((t) => tabletStatus(t, groups[t.id] || [])));
    const label = worst === "healthy" ? "ok" : worst;
    return `<div class="list-row"><span class="detail mono">${esc(name)}</span>
      <span class="muted">${ts.length} tablet(s)</span>
      ${pill(worst, label)}
    </div>`;
  }).join("");
  $("ov-tables").innerHTML = tableRows || `<div class="empty">no tables yet</div>`;

  // ---- balance: tablets per node ----
  const counts = memberIds.map((id) => (Object.values(groups).flat().filter((x) => nodeIdOf(x.node) === id).length));
  const downSet = new Set(memberIds.filter((id) => members[id] && members[id].status !== "Active"));
  const max = Math.max(...counts, 0);
  const min = counts.length ? Math.min(...counts) : 0;
  const spread = max ? Math.round(((max - min) / max) * 100) : 0;
  $("ov-balance-legend").textContent = memberIds.length ? `min ${min} · max ${max} · spread ${spread}%` : "";
  $("ov-balance").innerHTML = memberIds.map((id, i) => {
    const c = counts[i];
    const h2 = max ? Math.max(4, (c / max) * 56) : 4;
    return `<div class="bar${downSet.has(id) ? " down" : ""}" style="height:${h2}px" title="node ${esc(id)}: ${c} tablet(s)"></div>`;
  }).join("") || `<div class="empty">no nodes yet</div>`;
}
