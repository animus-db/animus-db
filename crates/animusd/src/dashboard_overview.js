"use strict";
// The Overview view: health banner, stat tiles, a top-N nodes list, a
// per-table breakdown (real data — the design's "Recent activity" panel is
// dropped, since there is no backend event log to back it), and a
// tablets-per-node balance chart. Depends on `dashboard_core.js` having
// loaded first (STATE, $, esc, pill, dot, nodeRaftkvId, nodeDisplayId,
// cpGroupsByTablet, tabletStatus, statusDotClass, computeHealth, activateTab).

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
  // `nodeDisplayId`, not `nodeRaftkvId` — the control leader may be a
  // control-ONLY node (ADR 0035), which has no `raftkv_id` at all (`null`);
  // `nodeDisplayId` falls back to `control_id` so a split deployment's
  // control leader shows a real node number instead of "node null".
  const bannerSummary = h.status === "critical"
    ? "No control-plane leader known — the cluster cannot accept metadata changes right now."
    : h.status === "degraded"
      ? `${h.leaderlessCount} tablet(s) leaderless · ${h.underReplicatedCount} under-replicated${h.downCount ? ` · ${h.downCount} node(s) down` : ""}.`
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
  const tiles = [
    { label: "Nodes", value: `${nodeCount - h.downCount}/${nodeCount}`, sub: h.downCount ? `${h.downCount} down` : "all up" },
    { label: "Tablets", value: `${tabletIds.length}`, sub: `across ${Object.keys((status && status.schemas && status.schemas.tables) || {}).length} table(s)` },
    { label: "Under-replicated", value: `${h.underReplicatedCount}`, sub: h.underReplicatedCount ? "needs attention" : "none" },
    { label: "Control plane", value: h.controlLeader ? `node ${esc(nodeDisplayId(h.controlLeader))}` : "—", sub: controlTermText },
  ];
  $("ov-tiles").innerHTML = tiles.map((t) =>
    `<div class="stat-tile"><div class="label">${esc(t.label)}</div><div class="value">${t.value}</div><div class="sub">${esc(t.sub)}</div></div>`
  ).join("");

  // ---- nodes list (top 6) ----
  // Every DATA member (tracked in replicated `Metadata`, keyed by raftkv id —
  // covers both a "data"-role and a "combined"-role node) plus every
  // CONTROL-ONLY node reachable via the `/admin/peers` fan-out (ADR 0035): a
  // control-only node is never a data member at all, so without the second
  // half it would never appear anywhere in the dashboard despite being a
  // real part of the cluster. Each row is tagged with its role so a split
  // deployment's control trio and data fleet read as what they are, not as
  // an undifferentiated node list.
  const dataRows = (memberIds.length ? memberIds : STATE.nodes.filter((n) => n.ok).map((n) => n.config.raftkv_id))
    .map((id) => {
      const m = members[id];
      const node = nodeByRaftkv(id);
      const up = m ? m.status === "Active" : !!(node && node.ok);
      const hostedCount = groups && Object.values(groups).flat().filter((x) => nodeRaftkvId(x.node) === id).length;
      const role = (node && node.config && node.config.role) || "data";
      return {
        id, role, up,
        detail: `${hostedCount} tablet(s)`,
        statusText: m ? m.status : (up ? "reachable" : "unreachable"),
      };
    });
  const controlOnlyRows = STATE.nodes
    .filter((n) => n.ok && n.config && n.config.role === "control")
    .map((n) => ({
      id: n.config.control_id, role: "control", up: true,
      detail: (n.raft && n.raft.is_leader) ? "control leader" : "control node",
      statusText: "reachable",
    }));
  const nodeRows = [...dataRows, ...controlOnlyRows].slice(0, 6).map((r) =>
    `<div class="list-row">${dot(r.up ? "ok-dot" : "bad-dot")}
      <span class="id mono">${esc(r.id)}</span>
      <span class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.03em">${esc(r.role)}</span>
      <span class="detail">${esc(r.detail)}</span>
      <span class="status-text" style="color:var(${r.up ? "--ok" : "--danger"})">${esc(r.statusText)}</span>
    </div>`).join("");
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
