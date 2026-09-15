# Closing a deferred prose sweep: rewrite in place as "new name (ADR NNNN's 'old name')" (2026-08-25, ADR 0056/0021/0052 rename follow-up)

The Ledger delivery's first PR deliberately left a prose sweep as a tracked
follow-up (see the "Renaming a branded UI surface..." entry above): dozens
of module-doc `//!`/`//` comments across `dashboard.rs`, `lib.rs`,
`console.rs`, every JS file header, every `console_*.rs` test header, two
crate `CLAUDE.md`s, and the root `CLAUDE.md` still said "AnimusDB Console"/
"AnimusDB Data Console" after the code was renamed to "animusd admin"/
"animusd console". Closing that follow-up found two things worth keeping
as a pattern:

1. **The old strings do not live in one obvious place.** They span doc
   comments (`rustdoc`-visible, so a stale one looks authoritative),
   ordinary `//`/`//!` file-header comments, and test-file header comments
   in three different crates — `rg -in "<old name>"` across the whole
   `crates/`+`docs/` tree (excluding the ADRs themselves and the
   engineering-lessons log, both of which are supposed to keep the
   historical name on record) is the only way to find the full set; a
   scan scoped to "the crate that owns the feature" misses the sibling
   crate's references (`animus-dynamo/CLAUDE.md`'s two mentions of "the
   Data Console" had nothing to do with `animusd` and would not have
   turned up in an `animusd`-scoped grep).
2. **Rewrite each hit as `new name (ADR NNNN's "old name")`, not a bare
   swap.** A plain find-and-replace to the new name loses the old name
   entirely, which breaks anyone (or any future grep) still searching for
   the term the ADR itself, an old commit message, or an old bug report
   uses; keeping the literal old string in quotes right next to the new
   one keeps both searchable from the same line, at the cost of a few
   extra words per comment. This is the same move ADR 0021's and ADR
   0052's own "naming disambiguation" amendments already made in prose;
   applying it at the comment-string level too avoids re-litigating "did
   we mean the old system or the new one" the next time someone greps for
   either name.

General rule: when a rename's brief says "sweep the deferred prose," grep
the *exact* old string(s) tree-wide first, do not trust the file list a
memory of "where the feature lives" would produce, and prefer
`new (old)`-shaped rewrites over silent replacement wherever the old name
might still be a search target (an ADR title, a test name, a changelog).
