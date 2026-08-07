"use strict";
// Shared state, fetch helpers, formatting utilities, theme, and tab routing
// for the AnimusDB Console. `dashboard_overview.js`, `dashboard_placement.js`,
// `dashboard_tablets.js`, `dashboard_browser.js`, and `dashboard_storage.js`
// load after this file and call into it (STATE, $, esc, getJSON, postJSON,
// pill, bytes, humanBytes, tokenBound, b64url, nodeRaftkvId, cpGroupsByTablet,
// autoSplitThresholds, tabletStatus, computeHealth, activateTab, gotoStorage);
// nothing here calls into them except `render()`, the single per-refresh
// entry point every view's render function hangs off of.
const SEED = window.location.origin;
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// State assembled each refresh.
let STATE = { status: null, nodes: [], peersErr: null };

// ---- theme ----
// A UI preference, not data — persisted client-side only. Both palettes are
// fully defined in dashboard.css; this just toggles which one applies.
const THEME_KEY = "animusdb-console-theme";
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  const btn = $("theme-toggle");
  if (btn) btn.textContent = theme === "light" ? "Light" : "Dark";
}
function initTheme() {
  const saved = localStorage.getItem(THEME_KEY);
  applyTheme(saved === "light" ? "light" : "dark");
}
function toggleTheme() {
  const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
  localStorage.setItem(THEME_KEY, next);
  applyTheme(next);
}

// Fetch JSON from `base + path`; throws on network/HTTP error.
async function getJSON(base, path) {
  const r = await fetch(base + path, { headers: { "accept": "application/json" } });
  if (!r.ok) throw new Error(r.status + " " + r.statusText);
  return r.json();
}

// POST JSON; returns {status, body} (body parsed as JSON when possible).
async function postJSON(base, path, obj) {
  const r = await fetch(base + path, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify(obj),
  });
  const text = await r.text();
  let body; try { body = JSON.parse(text); } catch { body = text; }
  return { status: r.status, body };
}

// Turn a "host:port" admin address into a fetchable origin. A wildcard bind host
// (0.0.0.0 / ::) isn't dialable from the browser — fall back to the page's host.
function baseFor(addr) {
  let host = addr, port = "";
  const i = addr.lastIndexOf(":");
  if (i >= 0) { host = addr.slice(0, i); port = addr.slice(i + 1); }
  host = host.replace(/^\[|\]$/g, "");
  if (host === "0.0.0.0" || host === "::" || host === "") host = window.location.hostname;
  const h = host.includes(":") ? "[" + host + "]" : host;
  return "http://" + h + (port ? ":" + port : "");
}

// Unpadded base64url of a byte array — the same encoding the server uses for
// binary partition tokens (admin.rs::token_base64).
function b64url(arr) {
  return btoa(arr.map((b) => String.fromCharCode(b)).join(""))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

// Render a byte sequence (JSON array of u8, or a string) for humans. Mirrors the
// server's key_display (admin.rs): a key with a binary 8-byte token prefix shows
// as <token-base64>:<remainder as text>; any other binary sequence shows whole as
// URL-safe base64.
function bytes(v) {
  if (v == null) return "∞";
  if (typeof v === "string") return v === "" ? "«start»" : v;
  if (Array.isArray(v)) {
    if (v.length === 0) return "«start»";
    const printable = (b) => b >= 32 && b < 127;
    if (v.every(printable)) return v.map((b) => String.fromCharCode(b)).join("");
    if (v.length >= 8 && !v.slice(0, 8).every(printable)) {
      return b64url(v.slice(0, 8)) + ":" + v.slice(8).map((b) => String.fromCharCode(b)).join("");
    }
    return b64url(v);
  }
  return String(v);
}

// A byte *count* (e.g. a tablet's total size) as a human-readable string
// ("842 B" / "1.2 KB" / "3.4 MB") — distinct from `bytes()` above, which
// renders a byte *array* (a key/value's raw content) for display. `null`
// passes through as `null` so callers can render their own placeholder.
function humanBytes(n) {
  if (n == null) return null;
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n;
  let i = -1;
  do {
    v /= 1024;
    i++;
  } while (v >= 1024 && i < units.length - 1);
  return `${v.toFixed(v < 10 ? 2 : 1)} ${units[i]}`;
}

// A tablet range boundary on the hash ring (ADR 0022): the first 8 key bytes are
// the big-endian Murmur3 partition token, shown as URL-safe base64 — matching the
// token prefix in the key views, so a boundary can be eyeballed against browsed
// keys; "…" marks elided bytes past the token. An empty start / null end is the
// ring's edge — the caller passes the all-00 / all-ff token.
function tokenBound(v, fill) {
  if (v == null || v === "") return fill;
  const arr = Array.isArray(v) ? v : Array.from(String(v), (c) => c.charCodeAt(0) & 0xff);
  if (arr.length === 0) return fill;
  const tok = b64url(arr.slice(0, 8));
  return arr.length > 8 ? `${tok}…` : tok;
}

function pill(cls, text) { return `<span class="pill ${esc(cls)}">${esc(text)}</span>`; }
function dot(cls) { return `<span class="dot ${esc(cls)}"></span>`; }

// ---- shared data-derivation helpers (used by more than one view) ----

function nodeByRaftkv(id) {
  return STATE.nodes.find((n) => n.ok && n.config && n.config.raftkv_id === id);
}
function nodeRaftkvId(n) { return n.config ? n.config.raftkv_id : "?"; }

// Collect every hosted CP group across reachable nodes, indexed by tablet id.
// Each group carries its own owning node's raftkv id (`g.node`, from
// `CpRaftView`) — resolve that to the real node, never the admin node that
// happened to answer the request. Under `--cluster N` dev mode every node's
// `/admin/raftkv` response lists *every* replica cluster-wide (the shared
// `ClusterEdgeState`), so tagging a group with whichever node's fetch
// returned it — rather than the group's real owner — mis-associates it: a
// `.find()` matching on that tag then lands on the wrong group's state,
// which is what made every replica dot in a tablet's row echo the same
// (first) group's leader/follower status. Dedupe by (tablet, owning node) so
// a group is counted once regardless of how many admin ports reported it.
function cpGroupsByTablet() {
  const map = {};
  const seen = new Set();
  for (const n of STATE.nodes) {
    if (!n.ok || !n.raftkv || !n.raftkv.groups) continue;
    for (const g of n.raftkv.groups) {
      const key = g.tablet + ":" + g.node;
      if (seen.has(key)) continue;
      seen.add(key);
      const node = nodeByRaftkv(g.node) || { config: { raftkv_id: g.node }, ok: false };
      (map[g.tablet] = map[g.tablet] || []).push({ node, g });
    }
  }
  return map;
}

// The `--auto-split K` (keys) and `--auto-split-bytes B` (bytes, ADR 0034)
// thresholds, from any reachable node's `/admin/config` — a single flag set,
// so every node agrees. Either field is `null` if that trigger isn't enabled
// for this run (both, either, or neither may be set).
function autoSplitThresholds() {
  const n = STATE.nodes.find((x) => x.ok && x.config);
  return {
    keys: n ? n.config.auto_split_threshold : null,
    bytes: n ? n.config.auto_split_bytes_threshold : null,
  };
}

// A tablet's derived status: `electing` if no elected leader among its
// hosting groups, `under-replicated` if fewer nodes are currently reachable
// and hosting than the tablet's configured replica count, else `healthy`.
// `gs` is `cpGroupsByTablet()[id] || []`.
function tabletStatus(t, gs) {
  if (!gs.some((x) => x.g.is_leader)) return "electing";
  const configured = (t.replicas || []).length;
  if (configured > 0 && gs.length < configured) return "under-replicated";
  return "healthy";
}
function statusDotClass(status) {
  if (status === "healthy" || status === "Active") return "ok-dot";
  if (status === "under-replicated" || status === "Leaving" || status === "Joining") return "warn-dot";
  if (status === "electing" || status === "Down") return "bad-dot";
  return "dim-dot";
}

// Derive an at-a-glance health rollup from data `loadAll()` already fetched —
// no new requests. A cluster with no control leader can't accept writes, so
// that alone is "critical". Degraded means an actual tablet needs attention
// (leaderless, or under-replicated per `tabletStatus` — fewer hosting groups
// than its configured replica count). A `Down` member is NOT by itself
// degrading: once the placement reconciler has repaired every tablet that
// member used to replicate onto a spare (every tablet back at its configured
// replication, none leaderless), the cluster is healthy again even though the
// dead node is still lingering in the roster undecommissioned — the failure
// detector/reconciler can keep trying to reach it for a while after data is
// already fully replicated, and that lingering `Down` member shouldn't hold
// the whole cluster at "degraded" once its data-loss risk is gone.
// `downCount` is still surfaced (nodes tile, banner text) for visibility.
function computeHealth() {
  const members = (STATE.status && STATE.status.members) || {};
  const memberIds = Object.keys(members);
  const downCount = Object.values(members).filter((m) => m.status === "Down").length;

  const tablets = (STATE.status && STATE.status.tablets) || {};
  const tabletIds = Object.keys(tablets);
  const groups = cpGroupsByTablet();
  const leaderlessCount = tabletIds.filter((id) => !(groups[id] || []).some((x) => x.g.is_leader)).length;
  const underReplicatedCount = tabletIds.filter((id) => tabletStatus(tablets[id], groups[id] || []) === "under-replicated").length;

  const controlLeader = STATE.nodes.find((n) => n.ok && n.raft && n.raft.is_leader);

  let status = "healthy";
  if (leaderlessCount > 0 || underReplicatedCount > 0) status = "degraded";
  if (!controlLeader) status = "critical";

  return {
    status, controlLeader,
    downCount, totalNodes: memberIds.length || STATE.nodes.length,
    leaderlessCount, underReplicatedCount, totalTablets: tabletIds.length,
  };
}

function renderHealthPill() {
  const h = computeHealth();
  const issues = h.leaderlessCount + h.underReplicatedCount;
  const label = h.status === "healthy" ? "Healthy"
    : h.status === "critical" ? "No control leader"
    : `Degraded · ${issues} issue${issues === 1 ? "" : "s"}`;
  $("health-pill").className = "health-pill " + h.status;
  $("health-pill").innerHTML = `<span class="dot"></span><span class="label">${esc(label)}</span>`;
}

function renderSidebarFoot() {
  const tablets = (STATE.status && STATE.status.tablets) || {};
  const members = (STATE.status && STATE.status.members) || {};
  const nodeCount = Object.keys(members).length || STATE.nodes.length;
  $("sidebar-foot").innerHTML =
    `<div>${esc(nodeCount)} node(s)</div><div>${esc(Object.keys(tablets).length)} tablet(s)</div>`;
}

async function loadAll() {
  let peers;
  try {
    peers = await getJSON(SEED, "/admin/peers");
  } catch (e) {
    peers = { admin_addrs: [SEED.replace(/^https?:\/\//, "")] };
    STATE.peersErr = String(e);
  }
  let status = null;
  try { status = await getJSON(SEED, "/admin/status"); } catch (e) { /* shown per-panel */ }

  const addrs = (peers.admin_addrs && peers.admin_addrs.length) ? peers.admin_addrs
    : [SEED.replace(/^https?:\/\//, "")];
  const nodes = await Promise.all(addrs.map(async (addr) => {
    const base = baseFor(addr);
    const node = { addr, base, ok: false };
    try {
      const [config, raft, raftkv, health] = await Promise.all([
        getJSON(base, "/admin/config"),
        getJSON(base, "/admin/raft").catch(() => null),
        getJSON(base, "/admin/raftkv").catch(() => null),
        getJSON(base, "/admin/health").catch(() => null),
      ]);
      Object.assign(node, { config, raft, raftkv, health, ok: true });
    } catch (e) {
      node.error = String(e);
    }
    return node;
  }));

  STATE = { status, nodes, peersErr: STATE.peersErr };
  render();
  $("updated").textContent = "updated " + new Date().toLocaleTimeString();
}

function render() {
  renderHealthPill();
  renderSidebarFoot();
  renderOverview();
  renderPlacement();
  renderTablets();
  renderStorageSelectors();
  renderBrowserTables();
  renderSeedTables();
  // Rebuild the Dynamo editor's skeleton only when the effective table
  // changed (first load, table created/dropped, or a manual switch) — never
  // on a routine refresh with the same selection, so in-progress edits survive.
  if (dyTable !== lastRenderedDyTable) {
    lastRenderedDyTable = dyTable;
    dynOnTable();
  }
}

// ---- tab routing (ADR 0021 follow-up 7: real URLs, refresh/back/forward preserve the tab) ----
// One flat set of views, presented as a sidebar (not a top nav row — the
// AnimusDB Console design's shell). Each keeps its own id and `/admin/ui/<tab>`
// path (the server's `is_ui_path` just prefix-matches, and existing
// bookmarks/tests target these exact leaves).
const TABS = ["overview", "placement", "tablets", "browser", "storage"];

function tabFromPath(path) {
  const m = /^\/admin\/ui\/([^/?#]+)/.exec(path);
  return m && TABS.includes(m[1]) ? m[1] : TABS[0];
}

let activeTab = TABS[0];

// Storage-tab deep-linking: the selected tablet + node ride along as
// `?tablet=&node=` on the `/admin/ui/storage` URL, so a refresh or a
// back/forward navigation lands back on the same detail instead of just the
// bare tab. The node identifier is its stable admin `base` origin — the same
// value already used as the `<option value>` in `updateStorageNodeOptions` —
// so no separate id scheme is needed. This is also what the Tablets detail
// panel's "Open in Storage →" link targets.
let pendingStorageParams = null;

function paramsFromLocation() {
  const p = new URLSearchParams(window.location.search);
  const tablet = p.get("tablet");
  const node = p.get("node");
  return (tablet || node) ? { tablet, node } : null;
}

// The storage tab's query string, from `override` if given, otherwise from
// the selects' current values (so a plain tab switch preserves whatever was
// already picked).
function storageQuery(override) {
  const tablet = (override && override.tablet != null) ? override.tablet : $("st-tablet").value;
  const node = (override && override.node != null) ? override.node : $("st-node").value;
  if (!tablet && !node) return "";
  const p = new URLSearchParams();
  if (tablet) p.set("tablet", tablet);
  if (node) p.set("node", node);
  return "?" + p.toString();
}

// Re-sync the address bar with the Storage tab's current selection. Called
// whenever the tablet/node dropdowns change while that tab is active, so
// manual browsing (not just a cross-link jump) is also bookmarkable.
function syncStorageUrl() {
  if (activeTab !== "storage") return;
  history.replaceState({ tab: "storage" }, "", "/admin/ui/storage" + storageQuery());
}

// Apply a pending `{tablet, node}` (from a deep-link URL or a cross-view jump)
// to the Storage selects and load its detail. Consumed once — a routine
// refresh afterward must not keep re-forcing the selection over a manual
// change, the same discipline `lastRenderedDyTable` uses for the Dynamo editor.
function applyPendingStorageParams() {
  if (!pendingStorageParams) return;
  const { tablet, node } = pendingStorageParams;
  pendingStorageParams = null;
  if (tablet != null) {
    const tsel = $("st-tablet");
    if ([...tsel.options].some((o) => o.value === String(tablet))) tsel.value = String(tablet);
  }
  updateStorageNodeOptions();
  if (node != null) {
    const nsel = $("st-node");
    if ([...nsel.options].some((o) => o.value === node)) nsel.value = node;
  }
  loadStorage();
}

// Jump to the Storage tab pre-selecting `tablet` and/or `node` (either may be
// `null` to leave that selector as-is) — used by cross-links from the
// Tablets/Placement/Overview views so "which tablet/node is this" never means
// re-picking the same ids in two dropdowns by hand.
function gotoStorage(tablet, node) {
  const params = { tablet: tablet != null ? String(tablet) : null, node: node || null };
  activateTab("storage", { push: true, storage: params });
  pendingStorageParams = params;
  applyPendingStorageParams();
}

// Show `tab` and, unless `opts.silent` (used from the popstate handler, where the
// browser already changed the URL), sync the address bar: `push` adds a history
// entry (nav click), otherwise the URL is normalized in place (initial load).
// `opts.storage` overrides the Storage tab's query params (a cross-link jump);
// otherwise they're read from the selects' current values.
function activateTab(tab, opts = {}) {
  if (!TABS.includes(tab)) tab = TABS[0];
  activeTab = tab;
  document.querySelectorAll(".sidebar button.navlink").forEach((x) => x.classList.toggle("active", x.dataset.tab === tab));
  document.querySelectorAll("main section").forEach((x) => x.classList.toggle("active", x.id === tab));
  if (!opts.silent) {
    const query = tab === "storage" ? storageQuery(opts.storage) : "";
    const url = "/admin/ui/" + tab + query;
    if (opts.push) history.pushState({ tab }, "", url);
    else history.replaceState({ tab }, "", url);
  }
}
