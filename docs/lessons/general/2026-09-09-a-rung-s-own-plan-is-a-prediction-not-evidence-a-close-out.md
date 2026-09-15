# A rung's own plan is a prediction, not evidence — a close-out must re-grep every named file against the landed PRs before citing the plan's outcome as fact (ADR 0061 rung L, C-12 close-out)

C-12's own opener (PR 1) named `control_membership_admin.rs` as an
11-of-12 conversion for PR 4a/4b, and every PR-by-PR appendix through PR
4d repeated that disposition without anyone re-checking it. The rung's
own close-out (PR 5) found, by running `git show --stat` on every landed
PR and `grep -c '#\[tokio::test' tests/control_membership_admin.rs`
against the branch tip, that **no landed PR ever touched the file** — it
sat at all 12 tests, entirely `ProdEnv`, the whole time. Nothing in any
PR's own commit message claimed otherwise; the gap existed only in the
difference between what the plan said would happen across the series and
what the individual PRs actually did, and closed the rung without
anyone comparing the two directly until the close-out did.

This is the same failure shape rung K's own close-out found for a
different kind of claim — a "why this stays `ProdEnv`" reason echoed
across multiple close-outs without being re-read against the current
test body (there, the stale "`SimCluster` has no metrics sink" doc
comment, copied three times; here, reading the same file's 12th test
overturned its own "permanent `--config` bring-up" label too, since the
test never parses a `--config FILE` at all). The generalized rule from
both: **a rung's own PR-by-PR plan, and every inherited reason a residual
"stays `ProdEnv`," are predictions and citations respectively — neither
is evidence.** A close-out (or any later rung that inherits a residual
figure) must independently re-verify, per file: (1) that a PR claiming to
convert or assess a file actually touched it (`git show --stat`, not the
commit message's own prose), and (2) that a residual's stated reason
still describes what the test's own current body actually does and needs
(read the test, not the label). Both checks are cheap — a `git show
--stat` and a file read per named item — and this rung is the second
consecutive one where skipping them would have closed with a materially
wrong record: one entire file silently mis-declared "converted," and one
of its own tests mis-declared "permanent" for the wrong reason.
