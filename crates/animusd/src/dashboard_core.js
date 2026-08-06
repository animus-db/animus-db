"use strict";
// Shared state, fetch helpers, formatting utilities, and tab routing.
// `dashboard_monitoring.js` and `dashboard_write.js` load after this file and
// call into it (STATE, $, esc, getJSON, postJSON, pill, bytes, tokenBound,
// b64url); nothing here calls into them except `render()`, which is the
// single per-refresh entry point every tab's render function hangs off of.
const SEED = window.location.origin;
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// State assembled each refresh.
let STATE = { status: null, nodes: [], peersErr: null };

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

// A minimal inline SVG sparkline over `values` (oldest first) — no charting
// library, matching the no-build-step constraint. Flat/empty series render as
// a flat midline rather than a division-by-zero NaN path. `style="stroke:…"`
// (not a bare `stroke=` attribute) so CSS custom properties resolve reliably.
function sparklineSvg(values, opts = {}) {
  const width = opts.width || 220;
  const height = opts.height || 32;
  if (!values.length) return `<span class="muted">no samples yet</span>`;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const stepX = values.length > 1 ? width / (values.length - 1) : 0;
  const points = values.map((v, i) =>
    `${(i * stepX).toFixed(1)},${(height - ((v - min) / span) * height).toFixed(1)}`).join(" ");
  return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" class="spark">
    <polyline points="${points}" fill="none" style="stroke:var(--accent)" stroke-width="1.5" />
  </svg>`;
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

function nodeByRaftkv(id) {
  return STATE.nodes.find((n) => n.ok && n.config && n.config.raftkv_id === id);
}

function render() {
  renderHealthStrip();
  renderNodes();
  renderMetricsHistorySelector();
  renderTablets();
  renderTopology();
  renderStorageSelectors();
  renderDynamoTables();
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
// One flat row of leaf tabs; each keeps its existing id and `/admin/ui/<tab>`
// path (the server's `is_ui_path` just prefix-matches, and existing
// bookmarks/tests target these exact leaves).
const TABS = ["nodes", "tablets", "storage", "write"];

function tabFromPath(path) {
  const m = /^\/admin\/ui\/([^/?#]+)/.exec(path);
  return m && TABS.includes(m[1]) ? m[1] : TABS[0];
}

let activeTab = TABS[0];

// Storage-tab deep-linking: the selected tablet + node ride along as
// `?tablet=&node=` on the `/admin/ui/storage` URL, so a refresh or a
// back/forward navigation lands back on the same detail instead of just the
// bare tab (the one gap the pre-existing `/admin/ui/<tab>` scheme left,
// noted in ADR 0021 follow-up 7). The node identifier is its stable admin
// `base` origin — the same value already used as the `<option value>` in
// `updateStorageNodeOptions` — so no separate id scheme is needed.
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

// Apply a pending `{tablet, node}` (from a deep-link URL or a cross-tab jump)
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
// `null` to leave that selector as-is) — used by cross-links from the Tablets
// and Nodes tables so "which tablet/node is this" never means re-picking the
// same ids in two dropdowns by hand.
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
  document.querySelectorAll(".primary button").forEach((x) => x.classList.toggle("active", x.dataset.tab === tab));
  document.querySelectorAll("main section").forEach((x) => x.classList.toggle("active", x.id === tab));
  if (!opts.silent) {
    const query = tab === "storage" ? storageQuery(opts.storage) : "";
    const url = "/admin/ui/" + tab + query;
    if (opts.push) history.pushState({ tab }, "", url);
    else history.replaceState({ tab }, "", url);
  }
}
