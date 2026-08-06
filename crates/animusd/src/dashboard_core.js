"use strict";
// Shared state, fetch helpers, formatting utilities, the global table
// selector, and tab routing. `dashboard_monitoring.js` and `dashboard_write.js`
// load after this file and call into it (STATE, $, esc, getJSON, postJSON,
// pill, bytes, tokenBound, b64url); nothing here calls into them except
// `render()`, which is the single per-refresh entry point every tab's
// render function hangs off of.
const SEED = window.location.origin;
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// State assembled each refresh.
let STATE = { status: null, nodes: [], peersErr: null };

// The globally-selected table (header dropdown) — every operation panel that
// targets one table (Dynamo op, bulk seed) reads this instead of keeping its
// own dropdown. CQL keeps typing table names into its statements (a script
// can target any table, or several) and stays out of this. `lastRenderedTable`
// tracks the value the Dynamo editor was last built for, so a routine refresh
// that leaves the selection unchanged doesn't clobber in-progress edits.
let selectedTable = "";
let lastRenderedTable;

// Every table name known anywhere: the replicated schema catalog (Dynamo +
// CQL) union the tablet map (a table can appear in one before the other —
// created but not yet written to, or vice versa via direct seeding).
function allTableNames() {
  const fromSchemas = Object.keys((STATE.status && STATE.status.schemas && STATE.status.schemas.tables) || {});
  const fromTablets = Object.values((STATE.status && STATE.status.tablets) || {}).map((t) => t.table).filter(Boolean);
  return [...new Set([...fromSchemas, ...fromTablets])].sort();
}

// Populate the header dropdown; drop the selection if it no longer exists.
function renderTableSelector() {
  const names = allTableNames();
  if (!names.includes(selectedTable)) selectedTable = "";
  const sel = $("global-table");
  sel.innerHTML = `<option value="">(select a table)</option>`
    + names.map((n) => `<option${n === selectedTable ? " selected" : ""}>${esc(n)}</option>`).join("");
  sel.value = selectedTable;
}
function onGlobalTableChange() {
  selectedTable = $("global-table").value;
  lastRenderedTable = selectedTable;
  dynOnTable();
  renderDynamoTables();
  renderSeedTables();
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
  renderTablets();
  renderStorageSelectors();
  renderTableSelector();
  renderDynamoTables();
  renderSeedTables();
  // Rebuild the Dynamo editor's skeleton only when the effective table
  // changed (first load, table created/dropped, or a manual switch) — never
  // on a routine refresh with the same selection, so in-progress edits survive.
  if (selectedTable !== lastRenderedTable) {
    lastRenderedTable = selectedTable;
    dynOnTable();
  }
}

// ---- tab routing (ADR 0021 follow-up 7: real URLs, refresh/back/forward preserve the tab) ----
// Two top-level parts (Monitoring / Actions), each a "group" of one or more
// leaf tabs. Leaf tabs keep their existing ids and `/admin/ui/<tab>` paths
// unchanged (the server's `is_ui_path` just prefix-matches, and existing
// bookmarks/tests target these exact leaves), so only the nav's presentation
// is grouped — routing is untouched.
const GROUPS = { monitoring: ["nodes", "tablets", "storage"], actions: ["write"] };
const TABS = Object.values(GROUPS).flat();
const lastTabInGroup = { monitoring: "nodes", actions: "write" };
function groupOf(tab) { return Object.keys(GROUPS).find((g) => GROUPS[g].includes(tab)); }

function tabFromPath(path) {
  const m = /^\/admin\/ui\/([^/?#]+)/.exec(path);
  return m && TABS.includes(m[1]) ? m[1] : TABS[0];
}

// Show `tab` and, unless `opts.silent` (used from the popstate handler, where the
// browser already changed the URL), sync the address bar: `push` adds a history
// entry (nav click), otherwise the URL is normalized in place (initial load).
function activateTab(tab, opts = {}) {
  if (!TABS.includes(tab)) tab = TABS[0];
  const group = groupOf(tab);
  lastTabInGroup[group] = tab;
  document.querySelectorAll(".primary button").forEach((x) => x.classList.toggle("active", x.dataset.group === group));
  document.querySelectorAll(".secondary").forEach((x) => { x.style.display = x.dataset.group === group ? "" : "none"; });
  document.querySelectorAll(".secondary button").forEach((x) => x.classList.toggle("active", x.dataset.tab === tab));
  document.querySelectorAll("main section").forEach((x) => x.classList.toggle("active", x.id === tab));
  if (!opts.silent) {
    const url = "/admin/ui/" + tab;
    if (opts.push) history.pushState({ tab }, "", url);
    else history.replaceState({ tab }, "", url);
  }
}
