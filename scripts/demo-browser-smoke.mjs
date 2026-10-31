#!/usr/bin/env node
// End-to-end browser check of the in-browser demo (ADR 0082). CI: .github/workflows/demo-browsers.yml.
//
//   scripts/build-web-demo.sh
//   python3 -m http.server 8000 --directory website &
//   NODE_PATH=$(npm root -g) node scripts/demo-browser-smoke.mjs chromium|firefox|webkit \
//       [--url http://127.0.0.1:8000/demo.html] [--shots DIR] [--exe /path/to/browser]
//
// Drives the page like a visitor: start, CreateTable, PutItem, GetItem, Query,
// Scan, PartiQL, the data console and operator dashboard in their frames (embedded
// mode: no logo, header, theme switch or warnings of their own; theme follows the
// shell), crash the control leader from the fault strip, watch the dashboard follow,
// write through a survivor, restart, partition, heal, add a node, speed slider, pause. Fails on any console error,
// page error or unexpected response. Needs Playwright's own browser build (a
// stock Firefox cannot be driven by Playwright).
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const { chromium, firefox, webkit } = require("playwright");

const args = process.argv.slice(2);
const name = args[0] && !args[0].startsWith("--") ? args[0] : "chromium";
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const url = opt("--url", "http://127.0.0.1:8000/demo.html");
const shots = opt("--shots", "");
const exe = opt("--exe", undefined);
const kind = { chromium, firefox, webkit }[name];
if (!kind) { console.error("unknown browser " + name); process.exit(2); }

const problems = [];
const steps = [];
const check = (label, ok, detail) => {
  steps.push((ok ? "PASS " : "FAIL ") + label + (detail ? " " + detail : ""));
  if (!ok) { problems.push(label); }
};

const browser = await kind.launch({ headless: true, executablePath: exe });
console.log("browser", name, browser.version());

async function session(viewport, scheme) {
  const ctx = await browser.newContext({ viewport, colorScheme: scheme });
  const page = await ctx.newPage();
  page.on("console", (m) => { if (m.type() === "error") { problems.push("console error: " + m.text()); } });
  page.on("pageerror", (e) => problems.push("page error: " + e.message));
  await page.goto(url);
  return page;
}
const shot = async (page, file) => { if (shots) { await page.screenshot({ path: `${shots}/${file}` }); } };
// The shell fills the viewport and never scrolls the page itself.
const pageFits = (page) => page.evaluate(() =>
  document.scrollingElement.scrollHeight <= window.innerHeight + 1 && document.scrollingElement.scrollWidth <= window.innerWidth);
// An embedded UI shows none of its own chrome.
async function embeddedClean(label, frameLoc, selfHidden) {
  const f = page.frameLocator(frameLoc);
  const embedded = await page.locator(frameLoc).evaluate((fr) => fr.contentDocument.documentElement.hasAttribute("data-embedded"));
  const visible = async (sel) => (await f.locator(sel).locator("visible=true").count());
  const bad = [];
  for (const sel of [".topbar", "svg.logo", ".theme-switch", ".banner", ".scope-note", "#refresh", "#auto"]) {
    if (await visible(sel)) { bad.push(sel); }
  }
  check(label + " embedded: no own header, logo, theme switch or warning", embedded && bad.length === 0, bad.join(" "));
  const h = await page.locator(frameLoc).evaluate((fr) => fr.getBoundingClientRect().height / window.innerHeight);
  check(label + " frame fills the viewport height", h > 0.6, h.toFixed(2));
}

const page = await session({ width: 1280, height: 900 }, "light");
await page.click("#start");
await page.waitForSelector("html[data-demo-ready='1']", { timeout: 120000 });
await page.waitForFunction(() => document.getElementById("ctl-leader").textContent.startsWith("node"), null, { timeout: 60000 });
check("start cluster, control leader elected", true, await page.textContent("#ctl-leader"));
check("one logo, strip visible, page does not scroll",
  (await page.locator("a.brand svg:visible").count()) === 1 && (await page.locator("#strip").isVisible()) && (await pageFits(page)));

async function send(example, node) {
  await page.selectOption("#example", example);
  if (node !== undefined) { await page.selectOption("#send-node", String(node)); }
  await page.click("#send");
  await page.waitForFunction(() => /^HTTP/.test(document.getElementById("resp-head").textContent), null, { timeout: 60000 });
  const head = await page.textContent("#resp-head");
  const body = await page.textContent("#resp");
  const code = Number(/HTTP (\d+)/.exec(head)[1]);
  await page.evaluate(() => { document.getElementById("resp-head").textContent = "reset"; });
  return { code, body };
}
for (const [ex, want] of [["create", "orders"], ["put", ""], ["get", "o-1001"], ["query", "o-1001"], ["scan", "o-1001"], ["partiql", "o-1001"]]) {
  const r = await send(ex);
  check(ex, r.code === 200 && r.body.includes(want), "HTTP " + r.code);
}
await shot(page, `${name}-desktop-light.png`);

// The real data console and operator dashboard, in iframes, against the same cluster.
{
  await page.click("#tab-console");
  const con = page.frameLocator("#frame-console");
  await con.locator("td.name a", { hasText: "orders" }).waitFor({ timeout: 60000 });
  check("data console lists the table made in the playground", true);
  await con.locator("td.name a", { hasText: "orders" }).click();
  await con.locator("body").getByText("customer_id").first().waitFor({ timeout: 60000 });
  check("data console opens the table (in-frame navigation)", true);
  await con.locator("a.tab-link", { hasText: "Items" }).click();
  await con.locator("body").getByText("o-1001").first().waitFor({ timeout: 60000 });
  check("data console shows the item written over the DynamoDB wire", true);
  await embeddedClean("data console", "#frame-console");
  await page.click("[data-theme-choice='dark']");
  await page.waitForFunction(() => document.getElementById("frame-console").contentDocument.documentElement.getAttribute("data-theme") === "dark");
  check("shell theme change reaches the open console frame", true);
  await page.click("[data-theme-choice='light']");
  await shot(page, `${name}-console.png`);

  await page.click("#tab-dashboard");
  const dash = page.frameLocator("#frame-dashboard");
  await dash.locator("body").getByText(/All 3 node\(s\) reachable/).waitFor({ timeout: 60000 });
  check("dashboard overview sees the cluster", true);
  await embeddedClean("dashboard", "#frame-dashboard");
  check("dashboard keeps its own navigation", (await dash.locator(".sidebar button.navlink").count()) >= 5);
  check("pages do not scroll with a UI open", await pageFits(page));
  await dash.getByText("orders").locator("visible=true").first().waitFor({ timeout: 60000 });
  check("dashboard lists the playground table", true);
  await dash.locator(".sidebar button.navlink", { hasText: "Tablets" }).click();
  await dash.getByText("orders").locator("visible=true").first().waitFor({ timeout: 60000 });
  check("dashboard tablets tab", true);
  await dash.locator(".sidebar button.navlink", { hasText: "Overview" }).click();
  await shot(page, `${name}-dashboard.png`);
  await page.click("#tab-cluster");
  await shot(page, `${name}-cluster.png`);
  check("cluster view: pages do not scroll", await pageFits(page));
  await page.click("#tab-requests");
}

const leaderText = await page.textContent("#ctl-leader");
const leader = Number(/\d+/.exec(leaderText)[0]);
await page.click(`button[aria-label='Crash node ${leader} (fault strip)']`);
await page.waitForFunction((l) => {
  const t = document.getElementById("ctl-leader").textContent;
  return /node \d+/.test(t) && t !== "node " + l;
}, leader, { timeout: 60000 });
const survivor = (leader + 1) % 3;
const r2 = await send("put2", survivor);
check("crash control leader from the strip, write through survivor", r2.code === 200, "HTTP " + r2.code);
// The dashboard (already loaded) follows the fault without a manual refresh.
await page.click("#tab-dashboard");
await page.frameLocator("#frame-dashboard").locator("body").getByText(/node\(s\) down/).waitFor({ timeout: 60000 });
check("dashboard follows the crash by itself", true);
await page.click(`button[aria-label='Restart node ${leader} (fault strip)']`);
await page.waitForFunction((l) => document.querySelectorAll("#chips .chip")[l].classList.contains("down") === false, leader, { timeout: 30000 });
check("restart from the strip", true);
await page.frameLocator("#frame-dashboard").locator("body").getByText(/All 3 node\(s\) reachable/).waitFor({ timeout: 60000 });
check("dashboard follows the restart", true);

await page.click("#tab-cluster");
await page.selectOption("#part-a", "0");
await page.selectOption("#part-b", "1");
await page.click("#partition");
check("partition", (await page.textContent("#partition-list")).includes("node 0 and node 1"));
await page.click("#heal");
check("heal", (await page.textContent("#partition-list")).includes("No partitions"));
await page.click("#grow");
await page.waitForFunction(() => document.querySelectorAll(".node").length === 4 && document.querySelectorAll("#chips .chip").length === 4, null, { timeout: 120000 });
check("add node", true);
await page.fill("#speed", "5");
await page.dispatchEvent("#speed", "input");
check("speed slider", (await page.textContent("#speed-out")) === "10x", await page.textContent("#speed-out"));
await page.click("#pause");
await page.waitForTimeout(400); // the clock text refreshes every 200 ms
const c1 = await page.textContent("#clock");
await page.waitForTimeout(600);
check("pause holds the clock", (await page.textContent("#clock")) === c1);
await page.click("#pause");
await page.waitForTimeout(600);
check("resume advances the clock", (await page.textContent("#clock")) !== c1);
await page.context().close();

const dark = await session({ width: 1280, height: 900 }, "dark");
await dark.click("#start");
await dark.waitForSelector("html[data-demo-ready='1']", { timeout: 120000 });
await shot(dark, `${name}-desktop-dark.png`);
check("dark: page does not scroll", await pageFits(dark));
await dark.context().close();
const mob = await session({ width: 390, height: 844 }, "light");
await mob.click("#start");
await mob.waitForSelector("html[data-demo-ready='1']", { timeout: 120000 });
const overflow = await mob.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
check("no horizontal page scroll at 390px", !overflow);
check("390px: page does not scroll, tabs reachable", (await pageFits(mob)) && (await mob.locator("#tab-dashboard").isVisible()));
await mob.click("#tab-cluster");
check("390px cluster view: no horizontal page scroll", !(await mob.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)));
await mob.click("#tab-requests");
await shot(mob, `${name}-mobile.png`);
await mob.context().close();
await browser.close();

console.log(steps.join("\n"));
if (problems.length) { console.log("PROBLEMS:\n" + problems.join("\n")); process.exit(1); }
console.log("OK");
