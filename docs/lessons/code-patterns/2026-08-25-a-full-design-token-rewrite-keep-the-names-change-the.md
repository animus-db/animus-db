# A full design-token rewrite ("keep the names, change the values") is only safe once you've counted every consumer, and a mockup's literal CSS can silently redefine what an existing token means (2026-08-25, ADR 0056, the "Ledger" visual system).

**A full design-token rewrite ("keep the names, change the values") is only
safe once you've counted every consumer, and a mockup's literal CSS can
silently redefine what an existing token means (2026-08-25, ADR 0056,
the "Ledger" visual system).** Rewriting `tokens.css` in place (new
palette, light-default instead of dark-default, glow-means-live replaced
by keyline-means-live) while keeping every token *name* stable — so the
two consumer stylesheets (`dashboard.css`, `console.css`, ~600 lines
combined) and every `dashboard_*.js`/`console.js` render function needed
zero call-site edits — only worked because a `grep -c "var(--$t)"` sweep
across every consumer was run *before* writing the new file, for every
single token name. That surfaced two things a values-only read of the old
file would have missed: (1) several tokens (`--glow-*`, `--live-underline`,
`--accent-hi`, `--shadow-recessed`, motion timing) had **zero** consumers
outside `tokens.css` itself, so they were safe to gut/repoint freely
without a wider search; (2) `button.primary`/`.btn-new`/`.btn-save`/
`.seg-group button.active`/`.seg-opt.selected` all filled with
`var(--accent)` + `var(--accent-ink)` under the old system, but the new
mockups' own literal CSS filled the equivalent controls with the *ink*
color (`background:#211f1a;color:#fafaf9` in the light mockup) — i.e. the
new system's "ONE accent role" rule (accent for links/underlines/hatch
only, never a solid fill) is a *component* decision the token file alone
can't express; it required rewriting those five call sites' `background`/
`color` properties, not just relying on `--accent`'s new value. Trusting
the token rename to carry that change silently would have shipped
accent-filled primary buttons that merely looked different (a legal but
spec-violating shade) instead of catching that the button recipe itself
had changed. **General rule**: before a "same names, new values" token
rewrite, grep every consumer for every token name being touched — a
zero-hit token is safe to redefine freely (or even fold into another
token), and a token whose *default recipe* the new mockups visibly
contradict (a filled control's authoritative markup uses a different
color than the token that used to supply it) needs its call sites edited
by hand, because the rename alone will compile clean and still be wrong.
(`crates/animusd/src/tokens.css`, `dashboard.css`, `console.css`.)
