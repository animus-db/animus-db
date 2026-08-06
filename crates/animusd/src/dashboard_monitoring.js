"use strict";
// Monitoring: the cluster-health strip, Nodes, Tablets, and Storage tabs.
// Depends on `dashboard_core.js` (STATE, $, esc, pill, getJSON, bytes,
// tokenBound, activateTab) having loaded first.

// Derive an at-a-glance health rollup from data `loadAll()` already fetched —
// no new requests. A cluster with no control leader can't accept writes, so
// that alone is "critical"; a leaderless tablet or a down node is "degraded"
// (something needs attention, but the cluster is otherwise serving).
function computeHealth() {
  const members = (STATE.status && STATE.status.members) || {};
  const memberIds = Object.keys(members);
  const downCount = Object.values(members).filter((m) => m.status === "Down").length;

  const tablets = (STATE.status && STATE.status.tablets) || {};
  const tabletIds = Object.keys(tablets);
  const groups = cpGroupsByTablet();
  const leaderlessCount = tabletIds.filter((id) => !(groups[id] || []).some((x) => x.g.is_leader)).length;

  const controlLeader = STATE.nodes.find((n) => n.ok && n.raft && n.raft.is_leader);

  let status = "healthy";
  if (leaderlessCount > 0 || downCount > 0) status = "degraded";
  if (!controlLeader) status = "critical";

  return {
    status, controlLeader,
    downCount, totalNodes: memberIds.length || STATE.nodes.length,
    leaderlessCount, totalTablets: tabletIds.length,
  };
}

function renderHealthStrip() {
  const h = computeHealth();
  const leaderFact = h.controlLeader
    ? `<span class="fact">control leader: node ${esc(nodeRaftkvId(h.controlLeader))}</span>`
    : `<span class="fact bad">no control leader known</span>`;
  const tabletsFact = h.totalTablets
    ? `<button class="fact${h.leaderlessCount > 0 ? " bad" : ""}" id="health-tablets">`
      + `${h.leaderlessCount} leaderless / ${h.totalTablets} tablet(s)</button>`
    : `<span class="fact">no tablets</span>`;
  const nodesFact = `<button class="fact${h.downCount > 0 ? " bad" : ""}" id="health-nodes">`
    + `${h.downCount} down / ${h.totalNodes} node(s)</button>`;
  $("health").innerHTML =
    `<span class="status ${h.status}">${h.status}</span>${leaderFact}${tabletsFact}${nodesFact}`;
  const tabletsBtn = $("health-tablets");
  if (tabletsBtn) tabletsBtn.addEventListener("click", () => activateTab("tablets", { push: true }));
  const nodesBtn = $("health-nodes");
  if (nodesBtn) nodesBtn.addEventListener("click", () => activateTab("nodes", { push: true }));
}

function renderNodes() {
  const status = STATE.status;
  const members = (status && status.members) || {};
  const ids = Object.keys(members).map(Number).sort((a, b) => a - b);
  const fallback = STATE.nodes.filter((n) => n.ok).map((n) => n.config.raftkv_id);
  const rows = (ids.length ? ids : fallback).map((id) => {
    const m = members[id];
    const node = nodeByRaftkv(id);
    const reach = node ? (node.ok ? pill("ok", "reachable") : pill("err", "unreachable")) : pill("Leaving", "no admin");
    const r = node && node.raft;
    const kv = node && node.raftkv;
    const leader = r && r.is_leader ? `<span class="leader">leader</span>` : (r ? "follower" : "—");
    return `<tr>
      <td class="mono">${esc(id)}</td>
      <td>${m ? pill(m.status, m.status) : "—"}</td>
      <td>${reach}</td>
      <td>${r ? esc(r.role) : "—"} ${leader === "—" ? "" : "· " + leader}</td>
      <td class="mono">${r ? esc(r.term) : "—"}</td>
      <td class="mono">${r ? `${r.commit_index}/${r.last_applied}/${r.durable_index}` : "—"}</td>
      <td>${kv ? (kv.hosts_cp ? `${kv.groups.length} group(s)` : "no") : "—"}</td>
      <td class="mono muted">${node ? esc(node.addr) : "—"}</td>
    </tr>`;
  }).join("");
  $("nodes-body").innerHTML = rows ? `<table>
    <thead><tr><th>node</th><th>status</th><th>admin</th><th>control raft</th>
      <th>term</th><th>commit/applied/durable</th><th>hosts CP</th><th>admin addr</th></tr></thead>
    <tbody>${rows}</tbody></table>`
    + (STATE.peersErr ? `<div class="err-line">peers fan-out fell back to this node only: ${esc(STATE.peersErr)}</div>` : "")
    : `<div class="empty">no members (control plane not ready?)</div>`;
}

// Collect every hosted CP group across reachable nodes, indexed by tablet id.
function cpGroupsByTablet() {
  const map = {};
  for (const n of STATE.nodes) {
    if (!n.ok || !n.raftkv || !n.raftkv.groups) continue;
    for (const g of n.raftkv.groups) {
      (map[g.tablet] = map[g.tablet] || []).push({ node: n, g });
    }
  }
  return map;
}

function renderTablets() {
  const status = STATE.status;
  const tablets = (status && status.tablets) || {};
  const ids = Object.keys(tablets).map(Number).sort((a, b) => a - b);
  const groups = cpGroupsByTablet();
  const rows = ids.map((id) => {
    const t = tablets[id];
    const gs = groups[id] || [];
    const lead = gs.find((x) => x.g.is_leader);
    const lg = lead && lead.g;
    const leaderCell = lead
      ? `<span class="leader">node ${esc(nodeRaftkvId(lead.node))}</span>`
      : `<span class="muted">unknown</span>`;
    const idx = lg ? `${lg.commit_index}/${lg.last_applied}/${lg.durable_index}` : "—";
    const voters = lg ? lg.voters.join(", ") : (t.replicas || []).join(", ");
    const tableCell = t.table
      ? `<span class="mono">${esc(t.table)}</span>`
      : `<span class="muted">—</span>`;
    return `<tr>
      <td class="mono">${esc(id)}</td>
      <td>${tableCell}</td>
      <td class="mono">${esc(tokenBound(t.range && t.range.start, "AAAAAAAAAAA"))} → ${esc(tokenBound(t.range && t.range.end, "__________8"))}</td>
      <td class="mono">${esc(t.epoch)}</td>
      <td class="mono">${esc((t.replicas || []).join(", "))}</td>
      <td>${leaderCell}</td>
      <td class="mono">${lg ? esc(lg.term) : "—"}</td>
      <td class="mono">${esc(idx)}</td>
      <td class="mono">${esc(voters)}</td>
    </tr>`;
  }).join("");
  $("tablets-body").innerHTML = rows ? `<table>
    <thead><tr><th>tablet</th><th>table</th><th>range</th><th>epoch</th><th>replicas (base)</th>
      <th>leader</th><th>term</th><th>commit/applied/durable</th><th>voters (group)</th></tr></thead>
    <tbody>${rows}</tbody></table>`
    : `<div class="empty">no tablets</div>`;
}

function nodeRaftkvId(n) { return n.config ? n.config.raftkv_id : "?"; }

function renderStorageSelectors() {
  const status = STATE.status;
  const tablets = status && status.tablets ? Object.keys(status.tablets).map(Number).sort((a, b) => a - b) : [1];
  const tsel = $("st-tablet");
  const prevT = tsel.value;
  tsel.innerHTML = tablets.map((id) => `<option value="${id}">tablet ${id}</option>`).join("");
  if (prevT && [...tsel.options].some((o) => o.value === prevT)) tsel.value = prevT;
  updateStorageNodeOptions();
}

// The storage endpoints (WAL/LSM/scan/key) are node-local — a node that hosts no
// replica of the tablet answers 404 — so offer only nodes whose /admin/raftkv view
// lists the selected tablet: the same registry the storage routes resolve
// (`local_cp`), so an offered node cannot 404. If none is reachable yet (a freshly
// provisioned or split tablet whose group is still forming), the dropdown is empty
// with a hint; the Load/Browse/inspect handlers already no-op on an empty node.
function updateStorageNodeOptions() {
  const tablet = Number($("st-tablet").value);
  const nsel = $("st-node");
  const prevN = nsel.value;
  const hostGroup = (n) => n.raftkv && (n.raftkv.groups || []).find((g) => g.tablet === tablet);
  const hosting = STATE.nodes.filter((n) => n.ok).filter(hostGroup);
  nsel.innerHTML = hosting.map((n) => {
    const tag = hostGroup(n).is_leader ? " · leader" : "";
    return `<option value="${esc(n.base)}">node ${esc(nodeRaftkvId(n))} (${esc(n.addr)})${tag}</option>`;
  }).join("");
  if (prevN && [...nsel.options].some((o) => o.value === prevN)) nsel.value = prevN;
  $("st-hint").textContent = hosting.length ? ""
    : "no reachable node hosts this tablet yet (group still forming?)";
}

async function loadStorage() {
  const tablet = $("st-tablet").value;
  const base = $("st-node").value;
  if (!base) return;
  $("wal-records-card").style.display = "none";
  // WAL
  try {
    const w = await getJSON(base, "/admin/storage/wal?tablet=" + tablet);
    if (w.backend === "memory" || w.segments == null) {
      $("wal-body").innerHTML = `<div class="empty">memory backend — no WAL</div>`;
    } else {
      const segs = w.segments.map((s) => `<tr>
        <td class="mono"><a href="#" data-seg="${esc(s.segment)}" class="seglink">${esc(s.segment)}</a></td>
        <td class="mono">${esc(s.bytes)}</td></tr>`).join("");
      $("wal-body").innerHTML = `<div class="muted">durable_seq ${esc(w.durable_seq)} · rotations ${esc(w.rotations)}</div>
        <table><thead><tr><th>segment</th><th>bytes</th></tr></thead><tbody>${segs}</tbody></table>`;
      document.querySelectorAll(".seglink").forEach((a) =>
        a.addEventListener("click", (e) => { e.preventDefault(); loadWalSegment(base, tablet, a.dataset.seg); }));
    }
  } catch (e) { $("wal-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
  // LSM
  try {
    const l = await getJSON(base, "/admin/storage/lsm?tablet=" + tablet);
    if (l.backend === "memory" || l.sstables == null) {
      $("lsm-body").innerHTML = `<div class="empty">memory backend — no SSTables</div>`;
    } else {
      const levels = (l.levels || []).map((x) => `L${x.level}:${x.tables}`).join("  ") || "—";
      const tbl = l.sstables.map((s) => `<tr>
        <td class="mono">${esc(s.seq)}</td><td class="mono">${esc(s.level)}</td>
        <td class="mono">${esc(bytes(s.min_key))} → ${esc(bytes(s.max_key))}</td>
        <td class="mono">${esc(s.min_version)}–${esc(s.max_version)}</td>
        <td class="mono">${esc(s.file_size)}</td><td>${s.has_bloom ? "✓" : ""}</td></tr>`).join("");
      $("lsm-body").innerHTML = `<div class="muted">levels ${esc(levels)} · memtable ${esc(l.memtable.keys)} keys / ${esc(l.memtable.approx_bytes)} B</div>`
        + (tbl ? `<table><thead><tr><th>seq</th><th>level</th><th>key range</th><th>versions</th><th>bytes</th><th>bloom</th></tr></thead><tbody>${tbl}</tbody></table>`
               : `<div class="empty">no sstables (all in memtable)</div>`);
    }
  } catch (e) { $("lsm-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function loadWalSegment(base, tablet, seg) {
  $("wal-seg").textContent = seg;
  $("wal-records-card").style.display = "";
  $("wal-records").innerHTML = `<div class="empty">loading…</div>`;
  try {
    const r = await getJSON(base, "/admin/storage/wal/segment?tablet=" + tablet + "&seg=" + seg);
    const recs = (r.records || []).map((x) => `<tr>
      <td>${esc(x.type)}</td><td class="mono">${esc(x.key ?? "")}</td>
      <td class="mono">${esc(x.version ?? "")}</td>
      <td class="mono">${esc(x.value_len ?? x.keys ?? x.ops ?? "")}</td></tr>`).join("");
    $("wal-records").innerHTML = recs
      ? `<table><thead><tr><th>type</th><th>key</th><th>version</th><th>len/keys/ops</th></tr></thead><tbody>${recs}</tbody></table>`
      : `<div class="empty">empty segment</div>`;
  } catch (e) { $("wal-records").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function inspectKey() {
  const tablet = $("st-tablet").value;
  const base = $("st-node").value;
  const key = $("key-input").value;
  if (!base || !key) { $("key-body").innerHTML = `<div class="empty">enter a key</div>`; return; }
  try {
    const k = await getJSON(base, "/admin/storage/key?tablet=" + tablet + "&key=" + encodeURIComponent(key));
    const disk = (k.disk_versions || []).map((d) =>
      `<tr><td class="mono">${esc(d.version)}</td><td>${d.tombstone ? pill("err", "tombstone") : pill("ok", "value")}</td></tr>`).join("");
    $("key-body").innerHTML = `<div class="muted">key <code>${esc(k.key)}</code> · live: ${k.live == null ? "<span class='muted'>absent</span>" : `<code>${esc(k.live)}</code>`}</div>`
      + (disk ? `<table><thead><tr><th>version</th><th>kind</th></tr></thead><tbody>${disk}</tbody></table>`
              : `<div class="empty">no on-disk versions</div>`);
  } catch (e) { $("key-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function browseKeys() {
  const tablet = $("st-tablet").value;
  const base = $("st-node").value;
  if (!base) { $("scan-body").innerHTML = `<div class="empty">pick a node</div>`; return; }
  const start = $("scan-start").value;
  const limit = $("scan-limit").value || "50";
  const qs = "/admin/storage/scan?tablet=" + tablet
    + "&start=" + encodeURIComponent(start) + "&limit=" + encodeURIComponent(limit);
  try {
    const r = await getJSON(base, qs);
    if (r.backend === "memory" && r.count === 0) {
      $("scan-body").innerHTML = `<div class="empty">no live keys (memory backend starts empty)</div>`;
      return;
    }
    const rows = (r.items || []).map((it) =>
      `<tr><td class="mono"><a href="#" class="keylink" data-key="${esc(it.key)}">${esc(it.key)}</a></td>
        <td class="mono">${esc(it.value)}</td><td class="mono">${esc(it.value_len)}</td></tr>`).join("");
    const more = r.truncated
      ? `<div class="muted">showing first ${esc(r.count)} (truncated at limit ${esc(r.limit)}); set “start ≥” past the last key to page on</div>`
      : `<div class="muted">${esc(r.count)} live key(s)</div>`;
    $("scan-body").innerHTML = rows
      ? more + `<table><thead><tr><th>key</th><th>value</th><th>bytes</th></tr></thead><tbody>${rows}</tbody></table>`
      : `<div class="empty">no live keys from “${esc(start) || "the beginning"}”</div>`;
    // Click a key to send it to the inspector below.
    document.querySelectorAll(".keylink").forEach((a) =>
      a.addEventListener("click", (e) => {
        e.preventDefault();
        $("key-input").value = a.dataset.key;
        inspectKey();
        $("key-input").scrollIntoView({ block: "nearest" });
      }));
  } catch (e) { $("scan-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}
