# Playwright's Chromium download is an egress-policy 403 in this sandbox, not a flaky mirror (apps skin restyle, ADR 0056)

`npx playwright install --with-deps chromium` gets as far as `apt-get`
installing the real OS deps (xvfb, libnss3, …) — those hosts are
allowlisted — then fails downloading the browser binary itself:
`playwright.azureedge.net`, `playwright-akamai.azureedge.net`, and
`playwright-verizon.azureedge.net` (all three CDN mirrors Playwright
tries in turn) each return a proxy-level `ERR_SOCKET_CLOSED`/connection
reset from Node's http client. `curl -sS
$HTTPS_PROXY/__agentproxy/status` (per this repo's own sandbox README)
shows the real cause: `recentRelayFailures` records `connect_rejected,
"gateway answered 403 to CONNECT (policy denial or upstream failure)"`
for exactly those three hosts — an organization egress-policy denial, not
a transient network failure, so retrying (a different mirror, a retry
loop, `--force`) wastes time for zero chance of success. `npm install
playwright` itself succeeds fine (npm's registry is allowlisted); it is
specifically the browser-binary CDN that is blocked. Practical
consequence: a task asking for a real rendered/screenshotted visual check
cannot always get one in this sandbox even when Node/npm/apt all work and
the task budget allows for it — check `/__agentproxy/status` for a `403`/
`connect_rejected` against the actual download host before spending time
on `apt-get`/`npm install` troubleshooting, and if it's a policy denial,
say so honestly and fall back to the next-best verification available
(built/served bytes inspection, a live server's `curl`'d output, code
review) rather than silently claiming a visual check that didn't happen —
the task instructions in this repo are explicit that an honest "couldn't
verify visually" beats a false claim.
