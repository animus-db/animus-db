"use strict";
// The Storage view: folded-in debug tools that don't fit the console's other
// views — per-tablet-per-node WAL/LSM inspection and a raw key
// browser/inspector. Ported from the pre-redesign dashboard (unchanged
// behavior, restyled), since the AnimusDB Console design doesn't include this
// level of manual storage debugging at all and it would otherwise be lost.
// (The bulk-seed tool used to live here too; it writes real DynamoDB items
// now, so it moved to the Data Browser's DynamoDB panel,
// `dashboard_browser.js`.) Depends on `dashboard_core.js` (STATE, $, esc,
// pill, getJSON, bytes, nodeIdOf, syncStorageUrl,
// applyPendingStorageParams, pendingStorageParams).

function renderStorageSelectors() {
  const status = STATE.status;
  const tablets = status && status.tablets ? Object.keys(status.tablets).map(Number).sort((a, b) => a - b) : [1];
  const tsel = $("st-tablet");
  const prevT = tsel.value;
  tsel.innerHTML = tablets.map((id) => `<option value="${id}">tablet ${id}</option>`).join("");
  if (prevT && [...tsel.options].some((o) => o.value === prevT)) tsel.value = prevT;
  updateStorageNodeOptions();
  updateControlStorageNodeOptions();
  renderSystemTableKindOptions();
  // A deep-linked tablet/node (from the URL on load, or a browser back/forward
  // into the Storage tab, or the Tablets view's "Open in Storage" link) is
  // applied once the options it needs actually exist.
  if (pendingStorageParams) applyPendingStorageParams();
}

// The control-plane system-keyspace storage section (ADR 0038 PR4) is scoped
// to nodes with a LOCAL control role — a control-only or combined node, never
// a data-only one (which has no local control `RaftCore`/engine at all, ADR
// 0035; its Storage tab isn't even shown, ROLE_TABS in `dashboard_core.js`).
// Independent of `updateStorageNodeOptions`'s per-tablet-hosting filter above:
// a control-only node hosts no CP tablet group, so it would never appear
// there even though it's exactly the node this section exists to surface.
function updateControlStorageNodeOptions() {
  const sel = $("ctl-node");
  const prev = sel.value;
  const nodes = STATE.nodes.filter((n) => n.ok && (n.role === "control" || n.role === "combined"));
  sel.innerHTML = nodes.map((n) =>
    `<option value="${esc(n.base)}">node ${esc(n.addr)} (${esc(n.role)})</option>`).join("");
  if (prev && [...sel.options].some((o) => o.value === prev)) sel.value = prev;
  $("ctl-hint").textContent = nodes.length ? "" : "no reachable control-role node";
}

// The system-keyspace BROWSE section (plan-syskv-ui, an ADR 0038 addendum) —
// nested in the same "Control system keyspace" card, reusing `ctl-node`'s
// control-role-only node selector so it never offers a node with no local
// control engine at all. The kind filter lists EVERY EntityKind, including
// the internal/legacy bookkeeping ones (Counter/CpMemberAddr) — full
// transparency by the project owner's own call, labeled rather than hidden,
// since hiding them would make "what does this node actually store" a lie
// by omission. (A third such kind, NodeIdAlloc — the ADR 0036 allocator's
// idempotency ledger — was removed in ADR 0040 PR4 along with the allocator
// itself.)
const SYSTEM_TABLE_KINDS = [
  ["", "(all kinds)"],
  ["tablet", "tablet"],
  ["member", "member"],
  ["schema", "schema"],
  ["policy", "policy"],
  ["node_addrs", "node_addrs"],
  ["keyspace", "keyspace"],
  ["merged", "merged"],
  ["counter", "counter (internal)"],
  ["cp_member_addr", "cp_member_addr (legacy)"],
];

// The forward-only pager's cursor for the CURRENTLY DISPLAYED page — `null`
// means "first page" (or "no further page"). `GET /admin/system-table`'s
// pagination is exclusive-after (ADR 0038 addendum), so there is no "previous
// page" without re-walking from the start — matching the plan's deliberately
// simple forward-only pager (this is a debug/inspection tool, not a general
// data browser).
let systemTableAfter = null;

function renderSystemTableKindOptions() {
  const sel = $("ctl-kind");
  if (sel.options.length) return; // a fixed list — populate once, not per-refresh
  sel.innerHTML = SYSTEM_TABLE_KINDS.map(([value, label]) =>
    `<option value="${esc(value)}">${esc(label)}</option>`).join("");
}

async function loadSystemTable(reset) {
  const base = $("ctl-node").value;
  if (!base) { $("ctl-browse-body").innerHTML = `<div class="empty">pick a control node</div>`; return; }
  if (reset) systemTableAfter = null;
  const kind = $("ctl-kind").value;
  let qs = "/admin/system-table?limit=50";
  if (kind) qs += "&kind=" + encodeURIComponent(kind);
  if (systemTableAfter) qs += "&after=" + encodeURIComponent(systemTableAfter);
  try {
    const r = await getJSON(base, qs);
    if (!r.available) {
      $("ctl-applied-index").textContent = "";
      $("ctl-next-page").disabled = true;
      $("ctl-browse-body").innerHTML = `<div class="empty">no control-plane system-keyspace engine on this node</div>`;
      return;
    }
    $("ctl-applied-index").textContent = "as of index " + r.applied_index;
    const rows = (r.items || []).map((it) => {
      const full = JSON.stringify(it.value, null, 2);
      const preview = full.length > 60 ? full.slice(0, 60).replace(/\n/g, " ") + "…" : full.replace(/\n/g, " ");
      return `<tr>
        <td class="mono">${esc(it.kind)}</td>
        <td class="mono">${esc(it.id)}</td>
        <td class="mono">${esc(it.version)}</td>
        <td class="mono"><details><summary>${esc(preview)}</summary><pre>${esc(full)}</pre></details></td>
      </tr>`;
    }).join("");
    const more = r.truncated
      ? `<div class="muted">showing ${esc(r.count)} (truncated at limit ${esc(r.limit)}) — Next page for more</div>`
      : `<div class="muted">${esc(r.count)} row(s)${kind ? " of kind " + esc(kind) : ""}</div>`;
    $("ctl-browse-body").innerHTML = rows
      ? more + `<table><thead><tr><th>kind</th><th>id</th><th>version</th><th>value</th></tr></thead><tbody>${rows}</tbody></table>`
      : `<div class="empty">no rows${kind ? " for kind " + esc(kind) : ""}</div>`;
    systemTableAfter = r.truncated ? r.next_after : null;
    $("ctl-next-page").disabled = !r.truncated;
  } catch (e) { $("ctl-browse-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
}

async function loadControlStorage() {
  const base = $("ctl-node").value;
  if (!base) { $("ctl-storage-body").innerHTML = `<div class="empty">pick a control node</div>`; return; }
  try {
    const r = await getJSON(base, "/admin/storage/control");
    if (!r.available) {
      $("ctl-storage-body").innerHTML = `<div class="empty">no control-plane system-keyspace engine on this node</div>`;
      return;
    }
    if (r.backend === "memory" || r.sstables == null) {
      $("ctl-storage-body").innerHTML = `<div class="muted">backend memory (--ephemeral) — no WAL/SSTables; metadata does not survive a restart</div>`;
      return;
    }
    const levels = (r.levels || []).map((x) => `L${x.level}:${x.tables}`).join("  ") || "—";
    const tbl = r.sstables.map((s) => `<tr>
      <td class="mono">${esc(s.seq)}</td><td class="mono">${esc(s.level)}</td>
      <td class="mono">${esc(bytes(s.min_key))} → ${esc(bytes(s.max_key))}</td>
      <td class="mono">${esc(s.min_version)}–${esc(s.max_version)}</td>
      <td class="mono">${esc(s.file_size)}</td><td>${s.has_bloom ? "✓" : ""}</td></tr>`).join("");
    $("ctl-storage-body").innerHTML =
      `<div class="muted">backend ${esc(r.backend)} · levels ${esc(levels)}
        · memtable ${esc(r.memtable.keys)} keys / ${esc(r.memtable.approx_bytes)} B
        · WAL durable_seq ${esc(r.wal.durable_seq)} · rotations ${esc(r.wal.rotations)}
        · ${esc((r.wal.segments || []).length)} segment(s)</div>`
      + (tbl ? `<table><thead><tr><th>seq</th><th>level</th><th>key range</th><th>versions</th><th>bytes</th><th>bloom</th></tr></thead><tbody>${tbl}</tbody></table>`
             : `<div class="empty">no sstables (all in memtable)</div>`);
  } catch (e) { $("ctl-storage-body").innerHTML = `<div class="err-line">${esc(e)}</div>`; }
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
    return `<option value="${esc(n.base)}">node ${esc(nodeIdOf(n))} (${esc(n.addr)})${tag}</option>`;
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
