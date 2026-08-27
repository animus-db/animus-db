# animusdb.io — the AnimusDB marketing site

Static HTML/CSS/JS on the "Ledger" design system (ADR 0056). **No build step,
no bundler** — edit a file, commit, and it ships. Google Fonts (Space
Grotesk + Martian Mono) is the one external request the site makes; every
other asset is local.

```
website/
  index.html            overview — what AnimusDB is, and where it stands
  how-it-works.html     the whole system on one page
  architecture.html     reference: the two planes, consistency, deployment, ports, limits
  compatibility.html    reference: DynamoDB operation-by-operation status
  performance.html      reference: the cost model (no benchmarks yet, and why)
  licence.html          reference: AGPL-3.0 scenario by scenario
  docs.html             documentation — concepts, architecture, API, operations
  install.html          build, run a cluster, connect, deploy
  articles/
    why-lock-in-compounds.html
    what-self-hosting-costs.html
    determinism.html
  assets/
    tokens.css    shared design tokens — byte-identical to crates/animusd/src/tokens.css
    site.css      the site's own skin (geometry) on top of the shared tokens
    site.js       theme switch (light/dark/system; light is the default), mobile nav,
                  copy buttons, docs scrollspy
    favicon.svg
  CNAME           custom domain: animusdb.io
  .nojekyll       Pages serves the files as-is
  robots.txt sitemap.xml
```

## Preview locally

```sh
python3 -m http.server 8000 --directory website
# then open http://127.0.0.1:8000
```

## Deploying

`.github/workflows/pages.yml` uploads `website/` to GitHub Pages on every push
to `main` that touches it. The repository needs **Settings → Pages → Source:
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
currently states no TLS on any port, no authentication beyond opt-in SigV4 on
the client DynamoDB port (ADR 0057), no format compatibility between
revisions, no tablet merge, and no Kubernetes operator.
`BatchGetItem`, `DeleteTable`, `ListTables`, on-demand backup/restore, and
continuous backups (PITR, ADR 0059) are all implemented — don't reintroduce
any of them as gaps without checking the code first.
