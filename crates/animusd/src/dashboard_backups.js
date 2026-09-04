"use strict";
// The Backups tab (ADR 0059, docs/roadmap.md U-02): a read-only render of
// the replicated backup (`/admin/backups`) and restore (`/admin/restores`)
// catalogs, plus four gated actions — Create backup, Delete backup, Restore
// from backup, and per-table PITR enable/disable — each behind a
// `window.confirm` and posted through the existing `/admin/data/dynamo`
// proxy (ADR 0021), the crate's one mutation idiom. Role-gated to control +
// combined exactly like Placement (`ROLE_TABS`, dashboard_core.js) — a
// data-only node has no local control-plane Raft state to read the
// replicated `Metadata.backups`/`restores`/`schemas[*].pitr` catalogs from.
// Depends on `dashboard_core.js` (STATE, $, esc, pill, postJSON, loadAll,
// humanBytes) and `dashboard_browser.js` (dynamoTables — the table picker
// for Create backup + the PITR table list, reusing the Data Browser's own
// table source rather than a second fetch).

// `created_wall_ms`/`enabled_wall_ms` are real epoch milliseconds
// (`env.wall_now()`-stamped, ADR 0051), unlike the mono `env.now()`-based
// durations `dashboard_streams.js`'s `monoDuration` renders — an ordinary
// calendar-time formatter is the honest one here.
function wallTime(ms) {
  return ms == null ? "—" : new Date(ms).toLocaleString();
}

function backupStatusPill(status) {
  const state = status && status.state;
  if (state === "AVAILABLE") return pill("ok", "AVAILABLE");
  if (state === "CREATING") return pill("forming", "CREATING");
  if (state === "EXPIRED") return `<span class="muted" style="opacity:.6">EXPIRED</span>`;
  if (state === "FAILED") return `<span title="${esc(status.reason || "")}">${pill("err", "FAILED")}</span>`;
  return `<span class="muted">${esc(state || "—")}</span>`;
}

function restoreStatusPill(status) {
  const state = status && status.state;
  if (state === "DONE") return pill("ok", "DONE");
  if (state === "SEEDING") return pill("forming", "SEEDING");
  if (state === "FAILED") return `<span title="${esc(status.reason || "")}">${pill("err", "FAILED")}</span>`;
  return `<span class="muted">${esc(state || "—")}</span>`;
}

function renderBackupCreateTableOptions() {
  const sel = $("bk-create-table");
  if (!sel) return;
  const names = Object.keys(dynamoTables()).sort();
  const cur = sel.value;
  sel.innerHTML = names.length
    ? names.map((n) => `<option${n === cur ? " selected" : ""}>${esc(n)}</option>`).join("")
    : `<option value="">(no tables)</option>`;
  $("bk-create-submit").disabled = !names.length;
}

function renderBackups() {
  renderBackupCreateTableOptions();

  const backups = (STATE.backups && STATE.backups.backups) || [];
  $("bk-summary").textContent = `${backups.length} backup${backups.length === 1 ? "" : "s"}`;

  const backupRows = backups.slice()
    .sort((a, b) => (b.created_wall_ms || 0) - (a.created_wall_ms || 0))
    .map((b) => {
      const tablets = b.tablets || [];
      const reported = tablets.filter((t) => t.reported).length;
      const canRestore = b.status && b.status.state === "AVAILABLE";
      const canDelete = b.status && b.status.state !== "CREATING";
      return `<tr>
        <td class="mono" title="${esc(b.backup_id)}">${esc(b.backup_name)}</td>
        <td class="mono">${esc(b.table)}</td>
        <td>${backupStatusPill(b.status)}</td>
        <td class="mono">${esc(wallTime(b.created_wall_ms))}</td>
        <td class="mono">${b.total_bytes != null ? esc(humanBytes(b.total_bytes)) : "—"}</td>
        <td class="mono">${tablets.length ? `${reported} / ${tablets.length}` : "—"}</td>
        <td>
          <button class="bk-restore" data-arn="${esc(b.backup_id)}" data-table="${esc(b.table)}"${canRestore ? "" : " disabled"}>Restore</button>
          <button class="danger-text bk-delete" data-arn="${esc(b.backup_id)}" data-name="${esc(b.backup_name)}"${canDelete ? "" : " disabled"}>Delete</button>
        </td>
      </tr>`;
    }).join("");
  $("bk-list-body").innerHTML = backupRows ? `<table>
    <thead><tr><th>Name</th><th>Table</th><th>Status</th><th>Created</th><th>Size</th><th>Tablets reported</th><th></th></tr></thead>
    <tbody>${backupRows}</tbody></table>` : `<div class="empty">no backups</div>`;
  document.querySelectorAll(".bk-restore").forEach((b) =>
    b.addEventListener("click", () => restoreBackup(b.dataset.arn, b.dataset.table)));
  document.querySelectorAll(".bk-delete").forEach((b) =>
    b.addEventListener("click", () => deleteBackup(b.dataset.arn, b.dataset.name)));

  // ---- Continuous backups (PITR), one row per table (TableSchema.pitr,
  // animus-control::schema — reusing dynamoTables(), the Data Browser's own
  // table source, rather than a second fetch) --------------------------
  const tables = dynamoTables();
  const tableNames = Object.keys(tables).sort();
  const pitrRows = tableNames.map((name) => {
    const spec = tables[name].pitr;
    const enabled = !!spec;
    return `<tr>
      <td class="mono">${esc(name)}</td>
      <td>${enabled ? pill("ok", "ENABLED") : `<span class="muted">DISABLED</span>`}</td>
      <td class="mono">${enabled ? esc(wallTime(spec.enabled_wall_ms)) : "—"}</td>
      <td><button class="pitr-toggle" data-table="${esc(name)}" data-enabled="${enabled ? "1" : "0"}">${enabled ? "Disable" : "Enable"}</button></td>
    </tr>`;
  }).join("");
  $("bk-pitr-body").innerHTML = pitrRows ? `<table>
    <thead><tr><th>Table</th><th>PITR</th><th>Enabled since</th><th></th></tr></thead>
    <tbody>${pitrRows}</tbody></table>` : `<div class="empty">create a table first</div>`;
  document.querySelectorAll(".pitr-toggle").forEach((b) =>
    b.addEventListener("click", () => togglePitr(b.dataset.table, b.dataset.enabled === "1")));

  // ---- Restores (`/admin/restores`, ADR 0059 §7/§10 — read-only, no
  // cancel/delete primitive exists on the backend yet) -------------------
  const restores = (STATE.restores && STATE.restores.restores) || [];
  const restoreRows = restores.map((r) => `<tr>
    <td class="mono">${esc(r.target_table)}</td>
    <td class="mono">${esc(r.source_table)}</td>
    <td>${r.source === "POINT_IN_TIME" ? pill("forming", "PITR") : pill("forming", "BACKUP")}</td>
    <td>${restoreStatusPill(r.status)}</td>
    <td class="mono">${r.tablet_state ? esc(r.tablet_state) : `<span class="muted">—</span>`}</td>
  </tr>`).join("");
  $("bk-restores-body").innerHTML = restoreRows ? `<table>
    <thead><tr><th>Target table</th><th>Source table</th><th>Kind</th><th>Status</th><th>Tablet</th></tr></thead>
    <tbody>${restoreRows}</tbody></table>` : `<div class="empty">no restores</div>`;
}

async function createBackup() {
  const table = $("bk-create-table").value;
  const name = $("bk-create-name").value.trim();
  if (!table) { $("bk-create-msg").textContent = "pick a table"; return; }
  if (!name) { $("bk-create-msg").textContent = "backup name is required"; return; }
  if (!window.confirm(`Create an on-demand backup of “${table}” named “${name}”?`)) return;
  $("bk-create-msg").textContent = "creating…";
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "CreateBackup",
    payload: { TableName: table, BackupName: name },
  });
  if (status >= 300) { $("bk-create-msg").textContent = (body && body.message) || `HTTP ${status}`; return; }
  $("bk-create-name").value = "";
  $("bk-create-msg").textContent = "";
  await loadAll();
}

async function deleteBackup(arn, name) {
  if (!window.confirm(`Delete backup “${name}”? This cannot be undone.`)) return;
  $("bk-action-msg").textContent = "";
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "DeleteBackup",
    payload: { BackupArn: arn },
  });
  if (status >= 300) {
    $("bk-action-msg").innerHTML = `<span class="err-line">${esc((body && body.message) || `HTTP ${status}`)}</span>`;
    return;
  }
  await loadAll();
}

async function restoreBackup(arn, sourceTable) {
  const target = window.prompt(`Restore “${sourceTable}” from this backup into a new table named:`, `${sourceTable}-restored`);
  if (!target) return;
  if (!window.confirm(`Restore backup into new table “${target}”?`)) return;
  $("bk-action-msg").textContent = "";
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "RestoreTableFromBackup",
    payload: { TargetTableName: target, BackupArn: arn },
  });
  if (status >= 300) {
    $("bk-action-msg").innerHTML = `<span class="err-line">${esc((body && body.message) || `HTTP ${status}`)}</span>`;
    return;
  }
  await loadAll();
}

async function togglePitr(table, currentlyEnabled) {
  const verb = currentlyEnabled ? "Disable" : "Enable";
  if (!window.confirm(`${verb} continuous backups (PITR) on “${table}”?`)) return;
  $("bk-action-msg").textContent = "";
  const { status, body } = await postJSON(SEED, "/admin/data/dynamo", {
    op: "UpdateContinuousBackups",
    payload: { TableName: table, PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: !currentlyEnabled } },
  });
  if (status >= 300) {
    $("bk-action-msg").innerHTML = `<span class="err-line">${esc((body && body.message) || `HTTP ${status}`)}</span>`;
    return;
  }
  await loadAll();
}
