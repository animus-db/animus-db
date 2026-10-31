#!/usr/bin/env node
// End-to-end browser check of the in-browser demo (ADR 0082). CI: .github/workflows/demo-browsers.yml.
//
//   scripts/build-web-demo.sh
//   python3 -m http.server 8000 --directory website &
//   NODE_PATH=$(npm root -g) node scripts/demo-browser-smoke.mjs chromium|firefox|webkit \
//       [--url http://127.0.0.1:8000/demo.html] [--shots DIR] [--exe /path/to/browser]
//
// Drives the page like a visitor: start, CreateTable, PutItem, GetItem, Query,
// Scan, PartiQL, crash the control leader, write through a survivor, restart,
// partition, heal, add a node, speed slider, pause. Fails on any console error,
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
const shot = async (page, file) => { if (shots) { await page.screenshot({ path: `${shots}/${file}`, fullPage: true }); } };

const page = await session({ width: 1280, height: 900 }, "light");
await page.click("#start");
await page.waitForSelector("html[data-demo-ready='1']", { timeout: 120000 });
await page.waitForFunction(() => document.getElementById("ctl-leader").textContent.startsWith("node"), null, { timeout: 60000 });
check("start cluster, control leader elected", true, await page.textContent("#ctl-leader"));

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

const leaderText = await page.textContent("#ctl-leader");
const leader = Number(/\d+/.exec(leaderText)[0]);
await page.click(`button[aria-label='Crash node ${leader}']`);
await page.waitForFunction((l) => {
  const t = document.getElementById("ctl-leader").textContent;
  return /node \d+/.test(t) && t !== "node " + l;
}, leader, { timeout: 60000 });
const survivor = (leader + 1) % 3;
const r2 = await send("put2", survivor);
check("crash control leader, write through survivor", r2.code === 200, "HTTP " + r2.code);
await page.click(`button[aria-label='Restart node ${leader}']`);
await page.waitForFunction((l) => document.querySelectorAll(".node")[l].classList.contains("down") === false, leader, { timeout: 30000 });
check("restart", true);

await page.selectOption("#part-a", "0");
await page.selectOption("#part-b", "1");
await page.click("#partition");
check("partition", (await page.textContent("#partition-list")).includes("node 0 and node 1"));
await page.click("#heal");
check("heal", (await page.textContent("#partition-list")).includes("No partitions"));
await page.click("#grow");
await page.waitForFunction(() => document.querySelectorAll(".node").length === 4, null, { timeout: 120000 });
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
await dark.context().close();
const mob = await session({ width: 390, height: 844 }, "light");
await mob.click("#start");
await mob.waitForSelector("html[data-demo-ready='1']", { timeout: 120000 });
const overflow = await mob.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
check("no horizontal page scroll at 390px", !overflow);
await shot(mob, `${name}-mobile.png`);
await mob.context().close();
await browser.close();

console.log(steps.join("\n"));
if (problems.length) { console.log("PROBLEMS:\n" + problems.join("\n")); process.exit(1); }
console.log("OK");
