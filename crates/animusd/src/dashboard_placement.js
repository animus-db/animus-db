"use strict";
// The Placement view: a grid of node cards (id, status, real member labels —
// never the design mockup's fabricated availability-zone strings; omitted
// when empty rather than shown as fake regions — tablet/leader counts). No
// CPU/mem/disk bars: nothing in this codebase samples host resources, and
// fabricating them would violate this admin tool's ground-truth-data ethos.
// Click a card to see that node's tablets. Depends on `dashboard_core.js`
// (STATE, $, esc, pill, dot, idSpan, nodeIdOf, cpGroupsByTablet, tabletStatus,
// gotoStorage) having loaded first.

let placementSelectedNode = null;

// This node's tablets, from the *configured* replica set (`t.replicas`) —
// not just the currently-reachable hosting groups — so a down node still
// shows what it's supposed to hold, not nothing.
function tabletsForNode(nodeId, tablets, groups) {
  return Object.keys(tablets)
    .filter((id) => (tablets[id].replicas || []).includes(nodeId))
    .map((id) => {
      const t = tablets[id];
      const gs = groups[id] || [];
      const rep = gs.find((x) => nodeIdOf(x.node) === nodeId);
      const role = rep ? (rep.g.is_leader ? "leader" : "follower") : "unreachable";
      return { id, table: t.table || "—", role, status: tabletStatus(t, gs) };
    });
}

function renderPlacement() {
  const status = STATE.status;
  const tablets = (status && status.tablets) || {};
  const groups = cpGroupsByTablet();
  const members = (status && status.members) || {};
  // ADR 0040 PR3: node ids are strings now — see `dashboard_overview.js`'s
  // identical comment for why a numeric sort/coercion would break here.
  const memberIds = Object.keys(members).sort();

  $("pl-summary").textContent = `${memberIds.length} node(s) · ${Object.keys(tablets).length} tablet(s)`;

  if (!memberIds.length) {
    $("pl-grid").innerHTML = `<div class="empty">no members yet</div>`;
    $("pl-node-detail").style.display = "none";
    return;
  }

  $("pl-grid").innerHTML = memberIds.map((id) => {
    const m = members[id];
    const node = nodeById(id);
    const up = m ? m.status === "Active" : !!(node && node.ok);
    const forNode = tabletsForNode(id, tablets, groups);
    const leaderCount = forNode.filter((t) => t.role === "leader").length;
    const labels = (m && m.labels) || {};
    const labelText = Object.entries(labels).map(([k, v]) => `${k}=${v}`).join(", ");
    return `<div class="placement-card${placementSelectedNode === id ? " selected" : ""}" data-node="${esc(id)}">
      <div class="head">
        <div class="idw">${dot(up ? "ok-dot" : "bad-dot")}${idSpan(id)}</div>
        <span class="status-text" style="color:var(${up ? "--ok" : "--danger"})">${esc(m ? m.status : (up ? "reachable" : "unreachable"))}</span>
      </div>
      <div class="labels">${labelText ? esc(labelText) : "&nbsp;"}</div>
      <div class="foot"><span>${forNode.length} tablet(s)</span><span>${leaderCount} leader(s)</span></div>
    </div>`;
  }).join("");

  document.querySelectorAll(".placement-card").forEach((el) =>
    el.addEventListener("click", () => {
      const id = el.dataset.node;
      placementSelectedNode = placementSelectedNode === id ? null : id;
      renderPlacement();
    }));

  if (placementSelectedNode == null || !members[placementSelectedNode]) {
    $("pl-node-detail").style.display = "none";
    return;
  }
  const forNode = tabletsForNode(placementSelectedNode, tablets, groups);
  const selNode = nodeById(placementSelectedNode);
  $("pl-node-title").innerHTML = `Tablets on node ${esc(placementSelectedNode)}
    ${consoleLink(selNode && selNode.ok ? selNode.base : null, placementSelectedNode)}`;
  $("pl-node-tablets").innerHTML = forNode.length ? `<table>
    <thead><tr><th>Tablet</th><th>Table</th><th>Role</th><th>Status</th></tr></thead>
    <tbody>${forNode.map((t) => `<tr class="clickable" data-tablet="${esc(t.id)}">
      <td class="mono">${esc(t.id)}</td><td>${esc(t.table)}</td>
      <td style="color:${t.role === "leader" ? "var(--accent)" : "var(--text2)"};font-weight:500">${esc(t.role)}</td>
      <td>${pill(t.status, t.status)}</td>
    </tr>`).join("")}</tbody></table>`
    : `<div class="empty">no tablets configured on this node</div>`;
  document.querySelectorAll("#pl-node-tablets tr[data-tablet]").forEach((tr) =>
    tr.addEventListener("click", () => gotoStorage(tr.dataset.tablet, null)));
  $("pl-node-detail").style.display = "";
}
