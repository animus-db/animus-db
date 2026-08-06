"use strict";
// The Storage view: folded-in debug tools that don't fit the console's other
// views — per-tablet-per-node WAL/LSM inspection, a raw key browser/inspector,
// and the bulk-seed tool for sharding tests. Ported from the pre-redesign
// dashboard (unchanged behavior, restyled), since the AnimusDB Console design
// doesn't include this level of manual storage debugging at all and it would
// otherwise be lost. Depends on `dashboard_core.js` (STATE, $, esc, pill,
// getJSON, postJSON, bytes, nodeRaftkvId, syncStorageUrl, applyPendingStorageParams,
// pendingStorageParams, loadAll).

function renderStorageSelectors() {
  const status = STATE.status;
  const tablets = status && status.tablets ? Object.keys(status.tablets).map(Number).sort((a, b) => a - b) : [1];
  const tsel = $("st-tablet");
  const prevT = tsel.value;
  tsel.innerHTML = tablets.map((id) => `<option value="${id}">tablet ${id}</option>`).join("");
  if (prevT && [...tsel.options].some((o) => o.value === prevT)) tsel.value = prevT;
  updateStorageNodeOptions();
  // A deep-linked tablet/node (from the URL on load, or a browser back/forward
  // into the Storage tab, or the Tablets view's "Open in Storage" link) is
  // applied once the options it needs actually exist.
  if (pendingStorageParams) applyPendingStorageParams();
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

// ---- Bulk seed (sharding test) ----
let seeding = false;
let seedTable = "";

// The seed target must be a table that *has a tablet* — the exact set
// `/admin/data/seed` accepts (it validates against the replicated tablet map),
// so both Dynamo and CQL `ks.table` tables qualify once provisioned. Refreshed
// by render() (which also runs mid-seed via loadAll), so keep the Seed
// button's disabled state in sync with both table validity and `seeding`.
// Auto-picks the first seedable table when none is selected yet or the
// current selection no longer qualifies.
function renderSeedTables() {
  const tablets = (STATE.status && STATE.status.tablets) || {};
  const seedable = [...new Set(Object.values(tablets).map((t) => t.table).filter(Boolean))].sort();
  if (!seedable.includes(seedTable)) seedTable = seedable[0] || "";
  const sel = $("seed-table");
  sel.innerHTML = seedable.length
    ? seedable.map((n) => `<option${n === seedTable ? " selected" : ""}>${esc(n)}</option>`).join("")
    : `<option value="">(none)</option>`;
  sel.value = seedTable;
  const validTable = !!seedTable;
  $("seed-no-tables").style.display = validTable ? "none" : "";
  $("seed-no-tables").textContent = seedable.length
    ? ""
    : "no table has a tablet yet — write to one once (Data Browser), then it can be seeded";
  $("seed-table").disabled = !seedable.length;
  $("seed-go").disabled = !validTable || seeding;
}

async function seedRun() {
  if (seeding) return;
  const table = seedTable;
  const prefix = $("seed-prefix").value;
  const vbytes = Math.max(0, parseInt($("seed-vbytes").value, 10) || 0);
  const total = Math.max(0, parseInt($("seed-total").value, 10) || 0);
  const chunk = Math.max(1, parseInt($("seed-chunk").value, 10) || 1000);
  if (!table) { $("seed-status").innerHTML = `<div class="empty">select a table above — every key names a table</div>`; return; }
  if (!total) { $("seed-status").innerHTML = `<div class="empty">set a total</div>`; return; }
  seeding = true;
  $("seed-go").disabled = true;
  $("seed-stop").disabled = false;
  let done = 0;
  const t0 = Date.now();
  // Refresh the Tablets view live so splits appear during a large seed — but
  // NEVER block a seed chunk on it. loadAll() serializes a full cluster-wide
  // admin fan-out; awaiting it every chunk would fold that latency into the
  // displayed keys/s rate and throttle throughput. Instead fire it non-blocking
  // and at most once per REFRESH_MS of wall-clock, and drop overlapping refreshes.
  const REFRESH_MS = 1000;
  let lastRefresh = 0;
  let refreshInFlight = false;
  const liveRefresh = () => {
    if (refreshInFlight) return;
    const now = Date.now();
    if (now - lastRefresh < REFRESH_MS) return;
    lastRefresh = now;
    refreshInFlight = true;
    loadAll().finally(() => { refreshInFlight = false; });
  };
  try {
    while (done < total && seeding) {
      const count = Math.min(chunk, total - done);
      const { status, body } = await postJSON(SEED, "/admin/data/seed",
        { table, count, start: done, key_prefix: prefix, value_bytes: vbytes });
      if (status >= 300) {
        $("seed-status").innerHTML = `<div class="err-line">${esc((body && body.error) || ("HTTP " + status))}</div>`;
        break;
      }
      const wrote = body.written || 0;
      done += wrote;
      const secs = Math.max(0.001, (Date.now() - t0) / 1000);
      const rate = Math.round(done / secs);
      $("seed-status").innerHTML =
        `<div class="muted">seeded ${done.toLocaleString()} / ${total.toLocaleString()} keys · ${rate.toLocaleString()}/s`
        + (body.error ? ` · <span class="err-line">last error: ${esc(body.error)}</span>` : "") + `</div>`;
      if (wrote === 0) break; // persistent failure — don't spin
      liveRefresh(); // non-blocking, throttled — splits show live without gating throughput
    }
    await loadAll(); // final authoritative refresh once seeding settles
    if (done >= total) $("seed-status").innerHTML += `<div>${pill("ok", "done — " + done.toLocaleString() + " keys")}</div>`;
    else if (!seeding) $("seed-status").innerHTML += `<div class="muted">stopped at ${done.toLocaleString()}</div>`;
  } catch (e) {
    $("seed-status").innerHTML = `<div class="err-line">${esc(e)}</div>`;
  } finally {
    seeding = false;
    renderSeedTables(); // recompute seed-go (stays disabled if no tables remain)
    $("seed-stop").disabled = true;
  }
}
