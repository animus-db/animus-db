"use strict";
// The Node view (ADR 0035 PR7): a data-only node's dedicated page instead of
// the cluster Console — this node's own identity, health, control-plane
// mirror status (the ADR 0035 §1/§5 watermark + leader hint, surfaced via
// `/admin/raft`'s `control_mirror`), hosted tablets (this node's own
// `/admin/raftkv`), a link to a reachable control/combined node's Console,
// and a trimmed storage-debug panel scoped to THIS node only — no node
// dropdown, unlike the Storage tab's cluster-wide picker, since there's only
// one node in scope here. Shown instead of the cluster Console on a
// data-only node, and appended last on a combined node (a combined node is
// also a data node). Depends on `dashboard_core.js` (SELF, ROLE, STATE, $,
// esc, pill, dot, idSpan, bytes, humanBytes, getJSON, nodeDisplayId).

function renderNode() {
  renderNodeIdentity();
  renderNodeHealth();
  renderNodeMirror();
  renderNodeTablets();
  renderConsoleLink();
  renderNodeTabletOptions();
}

function renderNodeIdentity() {
  const s = SELF;
  if (!s.ok || !s.config) {
    $("nd-identity").innerHTML = `<div class="empty">${s.error ? esc(s.error) : "loading…"}</div>`;
    $("nd-summary").textContent = "";
    return;
  }
  const c = s.config;
  const id = c.node_id;
  $("nd-summary").textContent = `node ${id} · ${c.role}`;
  const addrRows = Object.entries(c.addrs || {})
    .filter(([, v]) => v != null)
    .map(([k, v]) => `<div class="list-row"><span class="detail mono">${esc(k)}</span><span class="status-text mono">${esc(v)}</span></div>`)
    .join("");
  // U-06 (docs/roadmap.md): backup/segment store (redacted kind + root
  // path — `admin.rs::config_view`'s `StoreView`), the ADR 0048 quiescence
  // threshold, ADR 0057 auth state (access key ids only, never the
  // secret), and the resolved OTLP endpoint (ADR 0027). Same idiom as
  // `addrRows` above: a field this role/config doesn't have is `null` on
  // the wire and simply omitted from the list, not rendered as an empty row.
  const storeLabel = (store) => store == null
    ? null
    : store.path ? `${esc(store.kind)} · ${esc(store.path)}` : esc(store.kind);
  const configRows = [
    ["backup store", storeLabel(c.backup_store)],
    ["segment store", storeLabel(c.segment_store)],
    ["quiesce after", c.quiesce_after_ms != null ? `${esc(c.quiesce_after_ms)} ms` : null],
    ["SigV4 auth", c.auth_enabled == null ? null : c.auth_enabled
      ? `enabled (${(c.auth_access_key_ids || []).map(esc).join(", ") || "—"})`
      : "disabled"],
    ["OTLP endpoint", c.otlp_endpoint ? esc(c.otlp_endpoint) : null],
  ]
    .filter(([, v]) => v != null)
    .map(([k, v]) => `<div class="list-row"><span class="detail mono">${esc(k)}</span><span class="status-text mono">${v}</span></div>`)
    .join("");
  $("nd-identity").innerHTML = `
    <div class="section-head"><span class="title">This node</span>
      <span class="muted" style="font-size:10px;text-transform:uppercase;letter-spacing:.03em">${esc(c.role)}</span></div>
    <div class="stat-tiles" style="margin-bottom:14px">
      <div class="stat-tile"><div class="label">Node id</div><div class="value">${idSpan(id)}</div></div>
      <div class="stat-tile"><div class="label">Role</div><div class="value" style="font-size:16px;text-transform:capitalize">${esc(c.role)}</div></div>
    </div>
    ${addrRows}
    ${configRows}`;
}

function renderNodeHealth() {
  const s = SELF;
  const h = s.health;
  if (!s.ok || !h) {
    $("nd-health").innerHTML = `<div class="section-head"><span class="title">Health</span></div><div class="empty">loading…</div>`;
    return;
  }
  // `h.ok` is "control leader known" (`admin.rs::health`), not a replication
  // signal — labeling it "under-replicated" when false misattributed a
  // control-plane liveness gap to a data-risk state. "no control leader" is
  // the accurate read.
  $("nd-health").innerHTML = `
    <div class="section-head"><span class="title">Health</span>${pill(h.ok ? "healthy" : "err", h.ok ? "ready" : "no control leader")}</div>
    <div class="list-row">${dot(h.control_leader_known ? "ok-dot" : "bad-dot")}<span class="detail">control leader known</span><span class="status-text">${h.control_leader_known ? "yes" : "no"}</span></div>
    <div class="list-row">${dot(h.is_control_leader ? "ok-dot" : "dim-dot")}<span class="detail">this node is control leader</span><span class="status-text">${h.is_control_leader ? "yes" : "no"}</span></div>
    <div class="list-row">${dot(h.hosts_cp ? "ok-dot" : "dim-dot")}<span class="detail">hosts CP tablets</span><span class="status-text">${h.hosts_cp ? "yes" : "no"}</span></div>`;
}

// Control-plane mirror status (ADR 0035 §1/§5, `admin.rs::raft_view`'s
// `control_mirror`): only ever meaningfully non-default on a genuine
// data-only node (`ControlHandle::Remote`) — a control-only or combined node
// IS a control-plane voter, so its own Raft state (the `leader`/`term` this
// same `/admin/raft` response already carries) is the ground truth, not a
// polled mirror.
function renderNodeMirror() {
  const s = SELF;
  const r = s.raft;
  if (!s.ok || !r) {
    $("nd-mirror").innerHTML = `<div class="section-head"><span class="title">Control-plane mirror</span></div><div class="empty">loading…</div>`;
    return;
  }
  const cm = r.control_mirror || { watermark: 0, leader_hint: null, has_synced: false };
  const role = s.config && s.config.role;
  const isVoter = role !== "data";
  const synced = isVoter || cm.has_synced;
  const note = isVoter
    ? `<div class="muted" style="margin-top:6px">this node is a control-plane voter — no mirror is involved; the fields above already reflect its own Raft state.</div>`
    : "";
  $("nd-mirror").innerHTML = `
    <div class="section-head"><span class="title">Control-plane mirror</span>${pill(synced ? "healthy" : "warn", synced ? "synced" : "not yet synced")}</div>
    <div class="list-row"><span class="detail">applied-index watermark</span><span class="status-text mono">${esc(cm.watermark)}</span></div>
    <div class="list-row"><span class="detail">control leader</span><span class="status-text mono">${r.leader != null ? "node " + idSpan(r.leader) : "—"}</span></div>
    <div class="list-row"><span class="detail">leader address hint</span><span class="status-text mono">${cm.leader_hint ? esc(cm.leader_hint) : "—"}</span></div>
    ${note}`;
}

function renderNodeTablets() {
  const s = SELF;
  if (!s.ok) { $("nd-tablets").innerHTML = `<div class="empty">loading…</div>`; return; }
  const groups = (s.raftkv && s.raftkv.groups) || [];
  const rows = groups.map((g) => `<div class="list-row">
    ${dot(g.is_leader ? "ok-dot" : "dim-dot")}
    <span class="id mono">${esc(g.tablet)}</span>
    <span class="detail">${g.is_leader ? "leader" : "follower"} · term ${esc(g.term)} · applied ${esc(g.last_applied)}</span>
    <span class="status-text muted">${g.key_count != null ? esc(g.key_count.toLocaleString()) + " keys" : "—"}${g.byte_size != null ? " · " + esc(humanBytes(g.byte_size)) : ""}</span>
  </div>`).join("");
  $("nd-tablets").innerHTML = rows || `<div class="empty">this node hosts no tablets yet</div>`;
}

// Discovered from the same cross-cluster fan-out `loadAll()` already performs
// for every other view (`/admin/peers` + each peer's `/admin/config`, in
// `STATE.nodes`) — no extra probe needed; this is exactly the fan-out pattern
// the task called for, just reused rather than duplicated. `null` until the
// first full fan-out completes, which is fine: `SELF`'s own quick probe alone
// can never answer "is some OTHER node a control node," and the console link
// is explicitly allowed to resolve asynchronously, after this node's own page
// has already painted. Role prefers `n.config.role` (that node's own fetch)
// and falls back to `n.role` (from `/admin/peers` itself, ADR 0035 residual
// follow-up) so a candidate whose own `/admin/config` fetch hasn't resolved
// yet can still be picked — `n.base` is set regardless of fetch success, so
// the resulting link is always dialable.
function findConsoleNode() {
  return STATE.nodes.find((n) => {
    const role = (n.config && n.config.role) || n.role;
    return role === "control" || role === "combined";
  });
}

function renderConsoleLink() {
  const role = SELF.config && SELF.config.role;
  if (role && role !== "data") {
    $("nd-console-link").style.display = "none";
    return;
  }
  $("nd-console-link").style.display = "";
  const target = findConsoleNode();
  if (target) {
    $("nd-console-link").innerHTML =
      `<a href="${esc(target.base)}/admin/ui/overview" target="_blank" rel="noopener" class="link-text">Open cluster console (node ${esc(nodeDisplayId(target))}) →</a>`;
  } else if (STATE.nodes.length) {
    $("nd-console-link").innerHTML = `<span class="muted">no control node reachable from here right now.</span>`;
  } else {
    $("nd-console-link").innerHTML = `<span class="muted">checking for a reachable cluster console…</span>`;
  }
}

// ---- storage debug, trimmed to THIS node (no node dropdown) ---------------
// Mirrors the Storage tab's WAL/LSM/key/scan panels (`dashboard_storage.js`),
// simplified for the single-node case: `base` is always `SEED` (never a
// dropdown), and the tablet options are this node's own hosted tablets only
// (no cluster-wide tablet list — a data-only node's dedicated debug tools are
// only ever useful for what it actually hosts).
function renderNodeTabletOptions() {
  const groups = (SELF.raftkv && SELF.raftkv.groups) || [];
  const sel = $("nd-tablet-sel");
  const prev = sel.value;
  sel.innerHTML = groups.map((g) => `<option value="${esc(g.tablet)}">tablet ${esc(g.tablet)}</option>`).join("");
  if (prev && [...sel.options].some((o) => o.value === prev)) sel.value = prev;
  $("nd-hint").textContent = groups.length ? "" : "this node hosts no tablets yet";
}

async function loadNodeStorage() {
  const tablet = $("nd-tablet-sel").value;
  if (!tablet) return;
  $("nd-wal-records-card").style.display = "none";
  try {
    const w = await getJSON(SEED, "/admin/storage/wal?tablet=" + tablet);
    if (w.backend === "memory" || w.segments == null) {
      $("nd-wal-body").innerHTML = `<div class="empty">memory backend — no WAL</div>`;
    } else {
      const segs = w.segments.map((s) => `<tr>
        <td class="mono"><a href="#" data-seg="${esc(s.segment)}" class="nd-seglink">${esc(s.segment)}</a></td>
        <td class="mono">${esc(s.bytes)}</td></tr>`).join("");
      $("nd-wal-body").innerHTML = `<div class="muted">durable_seq ${esc(w.durable_seq)} · rotations ${esc(w.rotations)}</div>
        <table><thead><tr><th>segment</th><th>bytes</th></tr></thead><tbody>${segs}</tbody></table>`;
      document.querySelectorAll(".nd-seglink").forEach((a) =>
        a.addEventListener("click", (e) => { e.preventDefault(); loadNodeWalSegment(tablet, a.dataset.seg); }));
    }
  } catch (e) { $("nd-wal-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
  try {
    const l = await getJSON(SEED, "/admin/storage/lsm?tablet=" + tablet);
    if (l.backend === "memory" || l.sstables == null) {
      $("nd-lsm-body").innerHTML = `<div class="empty">memory backend — no SSTables</div>`;
    } else {
      const levels = (l.levels || []).map((x) => `L${x.level}:${x.tables}`).join("  ") || "—";
      const tbl = l.sstables.map((s) => `<tr>
        <td class="mono">${esc(s.seq)}</td><td class="mono">${esc(s.level)}</td>
        <td class="mono">${esc(bytes(s.min_key))} → ${esc(bytes(s.max_key))}</td>
        <td class="mono">${esc(s.min_version)}–${esc(s.max_version)}</td>
        <td class="mono">${esc(s.file_size)}</td><td>${s.has_bloom ? "✓" : ""}</td></tr>`).join("");
      $("nd-lsm-body").innerHTML = `<div class="muted">levels ${esc(levels)} · memtable ${esc(l.memtable.keys)} keys / ${esc(l.memtable.approx_bytes)} B</div>`
        + (tbl ? `<table><thead><tr><th>seq</th><th>level</th><th>key range</th><th>versions</th><th>bytes</th><th>bloom</th></tr></thead><tbody>${tbl}</tbody></table>`
               : `<div class="empty">no sstables (all in memtable)</div>`);
    }
  } catch (e) { $("nd-lsm-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function loadNodeWalSegment(tablet, seg) {
  $("nd-wal-seg").textContent = seg;
  $("nd-wal-records-card").style.display = "";
  $("nd-wal-records").innerHTML = `<div class="empty">loading…</div>`;
  try {
    const r = await getJSON(SEED, "/admin/storage/wal/segment?tablet=" + tablet + "&seg=" + seg);
    const recs = (r.records || []).map((x) => `<tr>
      <td>${esc(x.type)}</td><td class="mono">${esc(x.key ?? "")}</td>
      <td class="mono">${esc(x.version ?? "")}</td>
      <td class="mono">${esc(x.value_len ?? x.keys ?? x.ops ?? "")}</td></tr>`).join("");
    $("nd-wal-records").innerHTML = recs
      ? `<table><thead><tr><th>type</th><th>key</th><th>version</th><th>len/keys/ops</th></tr></thead><tbody>${recs}</tbody></table>`
      : `<div class="empty">empty segment</div>`;
  } catch (e) { $("nd-wal-records").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function inspectNodeKey() {
  const tablet = $("nd-tablet-sel").value;
  const key = $("nd-key-input").value;
  if (!tablet || !key) { $("nd-key-body").innerHTML = `<div class="empty">pick a tablet and enter a key</div>`; return; }
  try {
    const k = await getJSON(SEED, "/admin/storage/key?tablet=" + tablet + "&key=" + encodeURIComponent(key));
    const disk = (k.disk_versions || []).map((d) =>
      `<tr><td class="mono">${esc(d.version)}</td><td>${d.tombstone ? pill("err", "tombstone") : pill("ok", "value")}</td></tr>`).join("");
    $("nd-key-body").innerHTML = `<div class="muted">key <code>${esc(k.key)}</code> · live: ${k.live == null ? "<span class='muted'>absent</span>" : `<code>${esc(k.live)}</code>`}</div>`
      + (disk ? `<table><thead><tr><th>version</th><th>kind</th></tr></thead><tbody>${disk}</tbody></table>`
              : `<div class="empty">no on-disk versions</div>`);
  } catch (e) { $("nd-key-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function browseNodeKeys() {
  const tablet = $("nd-tablet-sel").value;
  if (!tablet) { $("nd-scan-body").innerHTML = `<div class="empty">pick a tablet</div>`; return; }
  const start = $("nd-scan-start").value;
  const limit = $("nd-scan-limit").value || "50";
  const qs = "/admin/storage/scan?tablet=" + tablet
    + "&start=" + encodeURIComponent(start) + "&limit=" + encodeURIComponent(limit);
  try {
    const r = await getJSON(SEED, qs);
    if (r.backend === "memory" && r.count === 0) {
      $("nd-scan-body").innerHTML = `<div class="empty">no live keys (memory backend starts empty)</div>`;
      return;
    }
    const rows = (r.items || []).map((it) =>
      `<tr><td class="mono"><a href="#" class="nd-keylink" data-key="${esc(it.key)}">${esc(it.key)}</a></td>
        <td class="mono">${esc(it.value)}</td><td class="mono">${esc(it.value_len)}</td></tr>`).join("");
    const more = r.truncated
      ? `<div class="muted">showing first ${esc(r.count)} (truncated at limit ${esc(r.limit)}); set “start ≥” past the last key to page on</div>`
      : `<div class="muted">${esc(r.count)} live key(s)</div>`;
    $("nd-scan-body").innerHTML = rows
      ? more + `<table><thead><tr><th>key</th><th>value</th><th>bytes</th></tr></thead><tbody>${rows}</tbody></table>`
      : `<div class="empty">no live keys from “${esc(start) || "the beginning"}”</div>`;
    document.querySelectorAll(".nd-keylink").forEach((a) =>
      a.addEventListener("click", (e) => {
        e.preventDefault();
        $("nd-key-input").value = a.dataset.key;
        inspectNodeKey();
        $("nd-key-input").scrollIntoView({ block: "nearest" });
      }));
  } catch (e) { $("nd-scan-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}
