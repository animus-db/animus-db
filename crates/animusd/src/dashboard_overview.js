"use strict";
// The Overview view: health banner, stat tiles, a top-N nodes list, a
// per-table breakdown (real data — the design's "Recent activity" panel is
// dropped, since there is no backend event log to back it), and a
// tablets-per-node balance chart. Depends on `dashboard_core.js` having
// loaded first (STATE, $, esc, pill, dot, nodeRaftkvId, cpGroupsByTablet,
// tabletStatus, statusDotClass, computeHealth, activateTab).

function renderOverview() {
  const status = STATE.status;
  const tablets = (status && status.tablets) || {};
  const tabletIds = Object.keys(tablets).map(Number).sort((a, b) => a - b);
  const groups = cpGroupsByTablet();
  const members = (status && status.members) || {};
  const memberIds = Object.keys(members).map(Number).sort((a, b) => a - b);
  const nodeCount = memberIds.length || STATE.nodes.length;

  const h = computeHealth();
  $("ov-summary").textContent = `${nodeCount} node(s) · ${tabletIds.length} tablet(s)`;

  // ---- health banner ----
  // Degraded is driven by tablet health (leaderless/under-replicated), not by
  // a lingering `Down` member — a dead node whose tablets have already been
  // repaired onto spares no longer degrades the cluster, even though it may
  // still be tracked (and re-contacted for a while) as a `Down` member until
  // it's decommissioned. `downCount` is still called out for context.
  const bannerSummary = h.status === "critical"
    ? "No control-plane leader known — the cluster cannot accept metadata changes right now."
    : h.status === "degraded"
      ? `${h.leaderlessCount} tablet(s) leaderless · ${h.underReplicatedCount} under-replicated${h.downCount ? ` · ${h.downCount} node(s) down` : ""}.`
      : h.downCount
        ? `${h.downCount} node(s) down, but all ${tabletIds.length} tablet(s) are fully replicated · control leader ${h.controlLeader ? "node " + esc(nodeRaftkvId(h.controlLeader)) : "—"}.`
        : `All ${nodeCount} node(s) reachable · ${tabletIds.length} tablet(s) replicated · control leader ${h.controlLeader ? "node " + esc(nodeRaftkvId(h.controlLeader)) : "—"}.`;
  $("ov-banner").className = "card health-banner " + h.status;
  $("ov-banner").innerHTML = `
    <div class="row" style="gap:10px">${dot((h.status === "healthy" ? "ok" : "bad") + "-dot")}
      <span style="font:600 15px var(--font-ui);color:${h.status === "healthy" ? "var(--text)" : "var(--danger)"}">${esc(h.status === "healthy" ? "Healthy" : h.status === "critical" ? "Critical" : "Degraded")}</span></div>
    <div class="muted" style="margin-top:6px">${esc(bannerSummary)}</div>`;

  // ---- stat tiles ----
  const controlTermText = h.controlLeader && h.controlLeader.raft ? `term ${h.controlLeader.raft.term}` : "—";
  const tiles = [
    { label: "Nodes", value: `${nodeCount - h.downCount}/${nodeCount}`, sub: h.downCount ? `${h.downCount} down` : "all up" },
    { label: "Tablets", value: `${tabletIds.length}`, sub: `across ${Object.keys((status && status.schemas && status.schemas.tables) || {}).length} table(s)` },
    { label: "Under-replicated", value: `${h.underReplicatedCount}`, sub: h.underReplicatedCount ? "needs attention" : "none" },
    { label: "Control plane", value: h.controlLeader ? `node ${esc(nodeRaftkvId(h.controlLeader))}` : "—", sub: controlTermText },
  ];
  $("ov-tiles").innerHTML = tiles.map((t) =>
    `<div class="stat-tile"><div class="label">${esc(t.label)}</div><div class="value">${t.value}</div><div class="sub">${esc(t.sub)}</div></div>`
  ).join("");

  // ---- nodes list (top 6) ----
  const nodeRows = (memberIds.length ? memberIds : STATE.nodes.filter((n) => n.ok).map((n) => n.config.raftkv_id))
    .slice(0, 6).map((id) => {
      const m = members[id];
      const node = nodeByRaftkv(id);
      const up = m ? m.status === "Active" : !!(node && node.ok);
      const hostedCount = groups && Object.values(groups).flat().filter((x) => nodeRaftkvId(x.node) === id).length;
      return `<div class="list-row">${dot(up ? "ok-dot" : "bad-dot")}
        <span class="id mono">${esc(id)}</span>
        <span class="detail">${hostedCount} tablet(s)</span>
        <span class="status-text" style="color:var(${up ? "--ok" : "--danger"})">${esc(m ? m.status : (up ? "reachable" : "unreachable"))}</span>
      </div>`;
    }).join("");
  $("ov-nodes").innerHTML = nodeRows || `<div class="empty">no members yet</div>`;

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
    const bad = ts.some((t) => tabletStatus(t, groups[t.id] || []) !== "healthy");
    return `<div class="list-row"><span class="detail mono">${esc(name)}</span>
      <span class="muted">${ts.length} tablet(s)</span>
      ${bad ? pill("under-replicated", "attention") : pill("healthy", "ok")}
    </div>`;
  }).join("");
  $("ov-tables").innerHTML = tableRows || `<div class="empty">no tables yet</div>`;

  // ---- balance: tablets per node ----
  const counts = memberIds.map((id) => (Object.values(groups).flat().filter((x) => nodeRaftkvId(x.node) === id).length));
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
