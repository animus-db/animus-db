"use strict";
// The Placement view: a grid of node cards (id, status, real member labels —
// never the design mockup's fabricated availability-zone strings; omitted
// when empty rather than shown as fake regions — tablet/leader counts). No
// CPU/mem/disk bars: nothing in this codebase samples host resources, and
// fabricating them would violate this admin tool's ground-truth-data ethos.
// Click a card to see that node's tablets. Depends on `dashboard_core.js`
// (STATE, $, esc, pill, dot, idSpan, nodeIdOf, cpGroupsByTablet, tabletStatus,
// gotoStorage) having loaded first.
//
// MRSC global tables (ADR 0075, G-01 G-c): a table with a replicated
// `schemas.tables[t].global` spec shows its Region, whether the leader this
// node can see sits outside the table's preferred-leader Region (witness
// Region included), and the preferred Region itself. All of it is derived from
// `/admin/status` (the full replicated `Metadata`) and the per-tablet groups
// already polled — nothing is invented, and a cluster with no global table
// renders exactly as before.

let placementSelectedNode = null;

// `{preferred_leader_region, witness, regions}` of an MRSC global table, else
// null. An MREC (eventual) table has no preferred leader or Region pin, so it
// is deliberately NOT returned here (it would read as "leader off preferred").
function globalSpecOf(status, table) {
  const g = rawGlobalOf(status, table);
  return g && !(g.replicas && g.replicas.length) ? g : null;
}

function rawGlobalOf(status, table) {
  const t = status && status.schemas && status.schemas.tables && status.schemas.tables[table];
  return (t && t.global) || null;
}

// MREC (ADR 0075 section 4, G-01 G-d): `"b ACTIVE, c CREATING"` of the table's
// non-local replicas from the replicated spec, else null. Shipper health (backlog,
// lag, last error) is node-local and lives on `/admin/global-tables`.
function mrecReplicaText(status, table) {
  const g = rawGlobalOf(status, table);
  if (!g || !g.replicas || !g.replicas.length) return null;
  const others = g.replicas.filter((r) => !r.local);
  if (!others.length) return null;
  return others.map((r) => `${r.region} ${String(r.status).toUpperCase()}`).join(", ");
}

// The `topology.kubernetes.io/region` label of member `nodeId`, else null.
function regionOfMember(members, nodeId) {
  const m = members && members[nodeId];
  return (m && m.labels && m.labels["topology.kubernetes.io/region"]) || null;
}

// Tablets of global tables whose visible leader is off the preferred Region.
function leadersOffPreferred(status, tablets, groups, members) {
  const off = [];
  Object.keys(tablets).forEach((id) => {
    const spec = globalSpecOf(status, tablets[id].table);
    if (!spec) return;
    const lead = (groups[id] || []).find((x) => x.g && x.g.is_leader);
    if (!lead) return;
    const region = regionOfMember(members, nodeIdOf(lead.node));
    if (region && region !== spec.preferred_leader_region) off.push(id);
  });
  return off;
}

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
      const spec = globalSpecOf(STATE.status, t.table);
      return {
        id, table: t.table || "—", role, status: tabletStatus(t, gs),
        preferred: spec ? spec.preferred_leader_region : null,
        mrec: mrecReplicaText(STATE.status, t.table),
        witness: !!(spec && spec.witness && spec.witness === regionOfMember(
          (STATE.status && STATE.status.members) || {}, nodeId)),
      };
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

  const offPreferred = leadersOffPreferred(status, tablets, groups, members);
  $("pl-summary").innerHTML = esc(`${memberIds.length} node(s) · ${Object.keys(tablets).length} tablet(s)`) +
    (offPreferred.length
      ? ` ${pill("warn", "leader off preferred: " + offPreferred.length)}`
      : "");

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
    <thead><tr><th>Tablet</th><th>Table</th><th>Role</th><th>Status</th><th>Global</th></tr></thead>
    <tbody>${forNode.map((t) => `<tr class="clickable" data-tablet="${esc(t.id)}">
      <td class="mono">${esc(t.id)}</td><td>${esc(t.table)}</td>
      <td style="color:${t.role === "leader" ? "var(--accent)" : "var(--text2)"};font-weight:500">${esc(t.role)}</td>
      <td>${pill(t.status, t.status)}</td>
      <td>${t.preferred ? esc("preferred " + t.preferred + (t.witness ? " · witness here" : ""))
        : (t.mrec ? esc("eventual · replicas " + t.mrec) : "—")}</td>
    </tr>`).join("")}</tbody></table>`
    : `<div class="empty">no tablets configured on this node</div>`;
  document.querySelectorAll("#pl-node-tablets tr[data-tablet]").forEach((tr) =>
    tr.addEventListener("click", () => gotoStorage(tr.dataset.tablet, null)));
  $("pl-node-detail").style.display = "";
}
