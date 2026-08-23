# ADR 0055 — One design system for the website and the in-product consoles ("Readout")

- **Status:** Accepted — implemented (token layer, typography and the panel
  recipe; per-component polish is a follow-up, see *Follow-ups* below).
- **Date:** 2026-08-23
- **Amends:** [ADR 0021](0021-web-dashboard.md) (dashboard styling),
  [ADR 0052](0052-data-console-port.md) (data console styling).

## Context

Three surfaces present AnimusDB to humans: the marketing site
(`website/`), the operator dashboard (`crates/animusd/src/dashboard.*`) and the
data console (`crates/animusd/src/console.*`). All three already shared a
*family* of values — the same near-black, the same warm paper, the same blue
accent — but they shared them by **copy-paste**: three near-identical `:root`
blocks, and buttons, pills, cards and tables defined three times over. Nothing
prevented them drifting, and they had already started to.

Two further problems were named when the system was commissioned:

1. **Typography had no character.** Every surface ran a system font stack, so
   nothing distinguished AnimusDB from any other Rust infrastructure project.
2. **The two densities are real and should stay.** The site is read at leisure,
   often on a phone; the consoles are scanned all day on a desk. A single
   density serves neither. The requirement was explicitly *"two design systems
   with a common base"*.

## Decision

### The direction: Readout

Of three directions explored, and three variants of the chosen one, the
selected direction is **Readout**: the interface as an instrument with a
housing. Panels are dark glass with a lit top edge; live values glow; figures
are promoted to display type in a wide mono.

The load-bearing rule is **glow means live**. A glow is reserved for state —
a live value, the focused panel's top edge, the one primary action, an active
status dot. Static content never glows, which is what makes a glow readable as
information rather than decoration. Anything that glows therefore needs a real
"is this live" signal behind it, not a style choice.

A glow on paper reads as a printing fault, so in the light theme every
illuminated element substitutes a 2px accent keyline in the same position
(`--live-underline`), and the glow tokens resolve to `none`. The information
survives the theme switch; the mechanism does not.

### Type

**Space Grotesk** (UI, display) and **Martian Mono** (figures, identifiers,
commands, column labels). Both are OFL variable fonts, so the Latin subset is
one file per family covering the whole weight range — 22 KB and 23 KB
respectively, not per weight.

The rule that keeps the pairing meaningful: *Space Grotesk carries every word a
human wrote; Martian Mono carries everything a machine produced.* A number a
person typed into a form is Space Grotesk; the same number read back from the
cluster is Martian Mono. Figures are always `tabular-nums`.

### One base, two skins

`tokens.css` holds what is genuinely identical everywhere: the colour palette
in both themes, the two families, status colours, glow, motion. It does **not**
hold geometry. Radius, spacing rhythm, control height and type sizes are the
*skin*, and each surface sets its own on top of the shared base:

| | site skin | app skin |
|---|---|---|
| Body | 17px / 1.65 | 13px / 1.5 |
| Radius | 12px (14 on panels) | 6px (10 on panels) |
| Controls | 44px | 28px |
| Rhythm | 8 · 16 · 24 · 32 · 48 · 64 · 96 | 4 · 8 · 14 · 22 · 32 |

### Why the token file is duplicated, and what stops it drifting

The site ships static files; the consoles embed their assets in the binary with
`include_str!` and serve them from the node. There is no runtime the two can
share, and ADR 0021 rules out the build step that would generate one. So
`tokens.css` exists twice, verbatim — and `dashboard::tokens_css_matches_website_copy`
fails the test gate if the two copies differ by a single byte. Duplication
without a check is exactly the drift this ADR exists to stop; the check is the
test.

Font *delivery* legitimately differs by surface and is therefore **not** part
of that check. The site links both families from Google Fonts, which is what a
public web page should do — cached across the web, nothing in the repo.

The consoles cannot, and this is a functional constraint rather than a stylistic
one: they are served from cluster nodes, and the intended deployment target is a
Kubernetes operator keeping node traffic cluster-internal. On an air-gapped or
egress-restricted cluster a `<link>` to fonts.googleapis.com renders the admin UI
in fallback faces, and on any cluster it makes every operator's browser call a
third party when they open an admin page. So they carry the identical faces as
base64 `data:` URIs — ~61 KB per stylesheet — which also keeps **ADR 0021's rule
unchanged for the surfaces it was written about**. The `.woff2` sources live at
`crates/animusd/src/fonts/`.

Serving real `.woff2` from the consoles would mean refactoring the HTTP layer's
static-asset responses from `String` to bytes — deliberately not done here.

Each console's served stylesheet is now `concat!` of three `include_str!`
literals — fonts, tokens, skin — so it stays a single compile-time constant
with no bundler and no extra route.

## Consequences

- ADR 0021's rule is intact for the consoles it governs: they still fetch
  nothing at runtime. The rule bans external fetches, not webfaces — embedding
  a face always satisfied it, which is why the previous system-font stacks were
  a self-imposed constraint rather than a required one.
- The binary grows by ~61 KB of base64 font data per console stylesheet.
- The dim grey tier now floors at **60% alpha**. Below that, the 9.5px mono
  labels this system uses fail WCAG AA on `#07080a`, so the old 0.4-alpha
  "quiet" greys are no longer available — quietness comes from size and weight.
- Drop shadows are gone system-wide. Depth is the panel gradient plus the lit
  top edge, which is why panels can sit flush without muddying.

## Follow-ups

1. Per-component Readout polish: the glow on live values in the dashboard's
   stat tiles and the console's row states, and the light-theme keyline
   substitution wired through each of them. This ADR lands the token layer,
   the typography and the panel recipe; the per-component pass is deliberately
   separate to keep the diff reviewable.
2. The readout tile as a shared component shape across both consoles.
3. Revisit serving real `.woff2` from the consoles if the static-asset layer
   ever grows a binary response path.
