# ADR 0056 — One design system for the website and the in-product consoles ("Readout")

- **Status:** Accepted — implemented (token layer, typography and the panel
  recipe; per-component polish is a follow-up, see *Follow-ups* below).
  **Revised in place by its own 2026-08-25 amendment**: the direction below
  ("Readout" — dark-first, glow-means-live) was replaced by a second pass,
  **"Ledger"** (light-first, keyline-means-live) — same file, same ADR
  number, per-component polish delivered as part of the revision rather
  than as this ADR's own still-open follow-up. See that amendment for the
  full decision and for which statements below it supersedes.
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

## Amendment (2026-08-25) — the "Ledger" revision replaces Readout in place

Readout shipped its token layer, typography and panel recipe (Accepted,
above) with per-component polish named as this ADR's own open follow-up
(#1). Before that follow-up landed, the direction itself was replaced —
**in place, same file, same ADR number** — by a second design pass:
**Ledger**. This amendment records the new decision and closes out
follow-ups #1 and #2 as delivered by it, rather than as originally
planned.

### The direction changes: Readout → Ledger

Where Readout was "the interface as an instrument with a housing" — dark
glass, a lit top edge, glowing live values — **Ledger is the interface as
a paper record**: flat chalk-paper surfaces, ink rules, stamped marks. The
brief that produced it was explicit that the Readout mockups read as
*decorative* rather than *systemic* — glow and gradient are effects
applied to a panel, not consequences of a rule — and asked for a direction
whose emphasis mechanism survives unchanged into a medium (paper, print,
a light theme) that a glow cannot.

**Light is now the default theme — this ADR's dark-first decision is
reversed.** `tokens.css`'s bare `:root` carries the light ("chalk paper")
values directly; `[data-theme="dark"]` and the guarded
`prefers-color-scheme: dark` media query both override to the dark ("ink")
values, exactly swapped from Readout's arrangement (`:root` dark,
`[data-theme="light"]` the override). The reasoning tracks the direction
change directly: an instrument housing is naturally dark (a lit panel in a
dark chassis), a paper ledger is naturally light — a system whose own
central metaphor is print reads as backwards defaulted to dark. **Dark
remains a first-class theme, not a filter or an afterthought**: every
token gets a real, independently-chosen dark value (not a computed
inverse), and the explicit-choice / system-preference switch mechanism
ADR 0021's theme toggle already drove is completely unchanged — only which
theme sits in the unqualified `:root` moved.

**Keyline-means-live replaces glow-means-live as the system's one emphasis
device, in both themes.** Readout's rule ("Decision," above) was glow on
dark, substituted for a keyline only in the light theme so a glow on paper
wouldn't read as a printing fault. Ledger drops the glow half entirely:
there is no dark-glass panel to glow inside of, so the 2px solid accent
underline (`--live-underline`, the `.live` class) is the *only* emphasis
device in either theme, not a light-theme fallback for one. The rule it
enforces is unchanged from Readout's own: reserved for real state (a live
value, an active nav/tab), never applied to static content — what changed
is that there is now one mechanism to get right, not two kept in sync
across a theme switch. The `--glow-*` tokens stay in `tokens.css`,
resolving to `none` in both themes; they were already dead weight in
Readout's own light theme and are unused everywhere now, kept only so
their names don't dangle for whatever consumer history refers to them.

**One accent, one role.** Ledger states explicitly what Readout's palette
implied but never wrote down: `--accent` has exactly one job across the
whole system — links, the live/active keyline, and the hatch fill below —
and never appears as a filled control background. A primary/CTA button
fills with **ink** (`--text`'s color, inverted for contrast), not accent;
see `docs/engineering-lessons.md`'s 2026-08-25 entry on the token-rewrite
project for the one place this rule and the mockups' own literal markup
disagreed with a naive "just repoint the token" read of the rename.

**Component language.** Readout's panel recipe — dark glass, a lit top
edge, drop shadows for depth (Consequences, above: "Depth is the panel
gradient plus the lit top edge") — is gone entirely; Ledger has **no**
panel gradients and **no** blur/soft shadows anywhere. In its place:

- Cards carry a flat `--surface` fill, a 1px rule border, and a 2px solid
  ink top rule (`border-top: 2px solid var(--text)`); section heads get a
  2px ink bottom rule instead. Depth comes from these hairline rules, not
  from gradient or shadow.
- Pills/badges are a **stamped outline** — 1–1.5px solid current-color,
  transparent fill, mono uppercase — replacing whatever filled/glowing
  badge treatment a live status used to get.
- A bar/gauge fill is **hatched**: a 45° `repeating-linear-gradient` of
  accent and `--hatch`, not a flat or gradient fill.
- A terminal/log-shaped surface (the dashboard's Streams tail-records log)
  gets an **ink-plate** treatment: a dark interior (`--plate-bg`,
  `--plate-text`, ...) that stays the *same* dark values in both the light
  and dark theme — the one deliberate exception to "light is the default
  surface" because a terminal reads as a terminal by staying dark
  regardless of the surrounding page — bordered and given a **hard, offset
  drop shadow** (`--plate-shadow: 5px 5px 0 rgba(...)`, no blur radius) for
  the one place in the system that does use a shadow at all: it reads as a
  stamped/pasted object, not as elevation.

**The old "60% alpha grey floor" is superseded by a fixed six-stop
ramp.** Readout's Consequences noted a WCAG-driven floor on its grey tier;
Ledger replaces the whole approach with the ink-alpha ramp snapped to six
fixed stops (`--ink-85`/`72`/`55`/`50`/`30`/`16`, aliased onto the
pre-existing `--text2`/`--text3` names) rather than a single floored
value — chosen directly against the new palette's own contrast ratios,
not derived from Readout's dark-on-`#07080a` figures, which no longer
apply now that the values they were computed against are gone.

### What is unchanged

- **Token *names* are stable.** Every name introduced by this ADR's
  original "One base, two skins" design — `--bg`, `--surface`, `--text`,
  `--accent`, `--ok`/`--warn`/`--danger`, the `--glow-*` family, `--panel`,
  `--shadow*` — still exists; only values (and, for the emphasis-device
  tokens, meaning) changed. No consumer stylesheet or script needed a
  rename.
- **The two-copy `tokens.css` mechanism, and the drift test, are
  unchanged.** `crates/animusd/src/tokens.css` and
  `website/assets/tokens.css` are still byte-identical copies, still
  enforced by `dashboard::tokens_css_matches_website_copy`; the "one base,
  two skins" split (site skin vs. app skin geometry) and the reasoning for
  duplicating the base file rather than sharing a build step are unchanged
  from the original Decision.
- **Fonts are unchanged**: Space Grotesk (UI, display) and Martian Mono
  (figures, identifiers, commands) remain the pairing and the *"a human
  wrote it vs. a machine produced it"* rule for which face applies where;
  delivery still splits by surface (Google Fonts on the public site,
  base64 `data:` URIs embedded in the two consoles) for the identical
  reason this ADR originally gave (ADR 0021's no-CDN-fetch rule, and the
  Kubernetes egress-restricted deployment target).

### Statements above now superseded

Kept above as originally written, describing the system as first designed
and shipped — the following are historical as of this amendment, not
current:

- "the selected direction is **Readout**... Panels are dark glass with a
  lit top edge; live values glow" (Decision, "The direction: Readout") —
  superseded; the shipped direction is Ledger, no dark-glass panels, no
  glow.
- "The load-bearing rule is **glow means live**" and the light-theme
  keyline-substitution paragraph immediately after it — superseded by
  keyline-means-live as the sole mechanism in both themes, above.
- "Drop shadows are gone system-wide. Depth is the panel gradient plus the
  lit top edge..." (Consequences) — Ledger has no panel gradient at all;
  depth comes from ink rules, and the one shadow the system does use (the
  ink-plate's hard offset shadow) is a deliberate exception, not a
  contradiction of "no shadows."
- "The dim grey tier now floors at 60% alpha..." (Consequences) —
  superseded by the fixed six-stop ink-alpha ramp, above.
- Follow-up #1 ("Per-component Readout polish... the light-theme keyline
  substitution wired through each of them") and follow-up #2 (the readout
  tile as a shared component shape) — both **delivered**, by this
  revision rather than as separately-scoped follow-up work: the dashboard
  and console skins were reskinned wholesale onto the Ledger component
  language above (cards, pills, hatch bars, the ink-plate) rather than
  receiving Readout's own glow/keyline polish. Follow-up #3 (real
  `.woff2` from the consoles) is untouched by this revision and stays
  open.

### Related renames

The operator dashboard and data console were also rebranded as part of
this same delivery — "AnimusDB Console" → **animusd admin**, "AnimusDB
Data Console" → **animusd console** — a naming change, not a design-system
one; see [ADR 0021](0021-web-dashboard.md)'s and
[ADR 0052](0052-data-console-port.md)'s own 2026-08-25 amendments.
