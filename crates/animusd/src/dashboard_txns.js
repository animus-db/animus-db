"use strict";
// The Transactions view (ADR 0018 §2/PR7, docs/roadmap.md U-01): a read-only
// render of `/admin/txns`' per-hosted-tablet transaction-tracker state —
// every `Pending` multi-participant transaction anchor and every
// `unresolved_decided` record this replica still holds, merged cluster-wide
// exactly like `/admin/raftkv` (`txnViewsByTablet`, dashboard_core.js).
// There is no manual-resolution action here — same as the JSON it renders
// (`CpTxnView`'s own doc, `admin.rs`): the `txn_resolver_loop`/
// `ClientCtx::txn_recover` machinery already drives every record here to a
// decision past `RECOVERY_GRACE` with no operator action needed, so this tab
// is purely observational, gated like `tablets` (ROLE_TABS).
// Depends on `dashboard_core.js` (STATE, $, esc, pill, idSpan, nodeIdOf,
// txnViewsByTablet).

// A record's own `created_wall_ms`/`age_ms` are already in milliseconds off
// each node's own clock — a small local formatter, since no other view here
// needs a duration (only byte/count formatters exist in dashboard_core.js).
function humanMs(ms) {
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  return `${m}m${Math.round(s - m * 60)}s`;
}

function txnTableCell(tablet) {
  const t = STATE.status && STATE.status.tablets && STATE.status.tablets[tablet];
  return t && t.table ? esc(t.table) : `<span class="muted">—</span>`;
}

function renderTxns() {
  const groups = txnViewsByTablet();
  const tabletIds = Object.keys(groups).map(Number).sort((a, b) => a - b);

  const pendingRows = [];
  const unresolvedRows = [];
  let pendingCount = 0;
  let unresolvedCount = 0;

  tabletIds.forEach((id) => {
    groups[id].forEach(({ node, tv }) => {
      pendingCount += tv.pending.length;
      unresolvedCount += tv.unresolved_decided.length;
      tv.pending.forEach((p) => {
        pendingRows.push(`<tr>
          <td class="mono">${esc(id)}</td>
          <td>${txnTableCell(id)}</td>
          <td>node ${idSpan(nodeIdOf(node), "mono")}</td>
          <td class="mono" title="${esc(p.txn_id)}">${esc(p.txn_id)}</td>
          <td class="mono">${esc(p.record_key)}</td>
          <td class="mono">${esc(humanMs(p.age_ms))}</td>
          <td>${p.past_grace ? pill("warn", "past grace") : pill("forming", "in-flight")}</td>
          <td class="mono">${p.intent_spans ? esc(p.intent_spans.join(", ")) : `<span class="muted">—</span>`}</td>
        </tr>`);
      });
      tv.unresolved_decided.forEach((u) => {
        const committed = u.outcome.startsWith("Committed");
        unresolvedRows.push(`<tr>
          <td class="mono">${esc(id)}</td>
          <td>${txnTableCell(id)}</td>
          <td>node ${idSpan(nodeIdOf(node), "mono")}</td>
          <td class="mono" title="${esc(u.txn_id)}">${esc(u.txn_id)}</td>
          <td class="mono">${esc(u.record_key)}</td>
          <td>${pill(committed ? "ok" : "err", committed ? "committed" : "aborted")}</td>
        </tr>`);
      });
    });
  });

  $("tx-summary").textContent = `${pendingCount} pending · ${unresolvedCount} unresolved decided`;
  $("tx-tiles").innerHTML = [
    { label: "Pending", value: `${pendingCount}`, sub: "in-flight, anchor still held" },
    { label: "Unresolved decided", value: `${unresolvedCount}`, sub: "decided, not yet cleaned up" },
    { label: "Tablets tracked", value: `${tabletIds.length}`, sub: "with any pending/unresolved state" },
  ].map((t) =>
    `<div class="stat-tile"><div class="label">${esc(t.label)}</div><div class="value">${t.value}</div><div class="sub">${esc(t.sub)}</div></div>`
  ).join("");

  $("tx-pending-body").innerHTML = pendingRows.length ? `<table>
    <thead><tr><th>Tablet</th><th>Table</th><th>Node</th><th>Txn</th><th>Record key</th><th>Age</th><th>Status</th><th>Intent spans</th></tr></thead>
    <tbody>${pendingRows.join("")}</tbody></table>` : `<div class="empty">no pending transactions</div>`;

  $("tx-unresolved-body").innerHTML = unresolvedRows.length ? `<table>
    <thead><tr><th>Tablet</th><th>Table</th><th>Node</th><th>Txn</th><th>Record key</th><th>Outcome</th></tr></thead>
    <tbody>${unresolvedRows.join("")}</tbody></table>` : `<div class="empty">no unresolved decided transactions</div>`;
}
