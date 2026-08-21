# animusdb.io — the AnimusDB marketing site

Static HTML/CSS/JS. **No build step, no bundler, no external requests** — the
same constraint the in-product consoles carry (ADR 0021): edit a file, commit,
and it ships. Fonts are system stacks; every asset is local.

```
website/
  index.html      landing page — what AnimusDB is and does
  docs.html       documentation — concepts, architecture, API, operations
  install.html    build, run a cluster, connect, deploy
  assets/
    site.css      design tokens (shared family with crates/animusd/src/dashboard.css)
    site.js       theme switch (light/dark/system), mobile nav, copy buttons, docs scrollspy
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
designed. When a capability lands or a gap closes, the pages that name it are:

- `index.html` — the compatibility table and the status section
- `docs.html` — `#api`, `#consistency`, and `#limits`
- `install.html` — the ports table

The known-gap list (`docs.html#limits`) is the load-bearing one: it currently
states no auth/TLS, no format compatibility between revisions, no backup/restore,
no `BatchGetItem`/`DeleteTable`/`ListTables`, no tablet merge, and no
Kubernetes operator.
