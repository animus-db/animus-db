# animusdb.io — the AnimusDB marketing site

Static HTML/CSS/JS on the "Ledger" design system (ADR 0056), restyled 2026-10-18 as an open layout: Archivo headings at 75% width, white ground, light grey code surface, no black blocks. **No bundler** — edit a file, commit, and it ships. The one build output is the in-browser demo's WebAssembly (`demo/pkg/`, gitignored; below). The site makes no third-party requests:
the two faces (Archivo, variable in weight and width, and JetBrains Mono for code only; latin subset, SIL OFL)
are self-hosted in `assets/fonts/`.

```
website/
  index.html            overview — what AnimusDB is, and where it stands
  how-it-works.html     the whole system on one page
  architecture.html     reference: the two planes, consistency, deployment, ports, limits
  compatibility.html    reference: DynamoDB operation-by-operation status
  performance.html      reference: the cost model, the benchmark methodology (no published numbers yet)
  licence.html          reference: AGPL-3.0 scenario by scenario
  docs.html             documentation — concepts, architecture, API, operations
  demo.html             live demo: a real cluster in the tab (wasm, virtual time; ADR 0082)
  install.html          docker run, build, run a cluster, connect, deploy
  articles/
    why-lock-in-compounds.html
    what-self-hosting-costs.html
    determinism.html
  assets/
    tokens.css    shared design tokens — byte-identical to crates/animusd/src/tokens.css
    site.css      the site's own skin (geometry) on top of the shared tokens
    demo.js       the demo page's behaviour (loads demo/pkg lazily, drives the virtual clock)
    demo.css      the demo page's layout
    site.js       theme switch (light/dark/system; light is the default),
                  copy buttons, docs scrollspy
    favicon.svg
    fonts/        self-hosted WOFF2 faces + their OFL licences
    og-card.png   1200×630 social card (og:image) for every page
  CNAME           custom domain: animusdb.io
  .nojekyll       Pages serves the files as-is
  404.html        served by Pages for unknown paths
  robots.txt sitemap.xml
```

## Preview locally

```sh
python3 -m http.server 8000 --directory website
# then open http://127.0.0.1:8000
```

## The in-browser demo

`demo.html` runs `crates/animus-web` (the real request path on the deterministic
simulator, ADR 0082) compiled to WebAssembly. Build output goes to
`website/demo/pkg/` and is **not committed**; without it the page loads but
"Start cluster" reports an error. To preview locally:

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version "$(awk '/^name = "wasm-bindgen"$/ {getline; gsub(/[^0-9.]/,""); print; exit}' Cargo.lock)" --locked
scripts/build-web-demo.sh
python3 -m http.server 8000 --directory website
```

Everything the page says about what is real, simulated and unsupported must stay
true of `crates/animusd/src/embedded.rs` (its module doc is the source).

## Deploying

`.github/workflows/pages.yml` builds the demo wasm and uploads `website/` to GitHub Pages on every push
to `main` that touches `website/` or the crates. The repository needs **Settings → Pages → Source:
"GitHub Actions"** set once, and `animusdb.io` pointed at GitHub Pages in DNS
(four `A` records for the apex, or a `CNAME` to `animus-db.github.io` for a
subdomain). Delete `website/CNAME` if the custom domain is not used — otherwise
Pages will keep trying to serve the site there.

## Keeping it honest

Product claims on these pages track what is actually implemented, not what is
designed — verify against the code (`crates/animusd/src/dynamo.rs`, the ADR
index) before propagating a claim, never from memory of what an older page
said. When a capability lands or a gap closes, the pages that name it are:

- `index.html` — the status cards (`#status`) and the warning box
- `architecture.html` — `#consistency`, `#day2`, and `#limits`
- `compatibility.html` — the operation-by-operation table
- `docs.html` — `#api`, `#consistency`, and `#limits`
- `install.html` — the ports table

The known-limits list (`architecture.html#limits`) is the load-bearing one: it
currently states that TLS is config-gated and off by default (ADR 0064), no
authentication beyond opt-in SigV4 on the client DynamoDB port (ADR 0057), no
rolling upgrades or mixed-version clusters (a full-cluster stop-upgrade-restart
is supported), restore always creates a new table, no tablet merge, no
at-scale benchmarks, and no production use.
`BatchGetItem`, `DeleteTable`, `ListTables`, on-demand backup/restore, and
continuous backups (PITR, ADR 0059) are all implemented — don't reintroduce
any of them as gaps without checking the code first.
