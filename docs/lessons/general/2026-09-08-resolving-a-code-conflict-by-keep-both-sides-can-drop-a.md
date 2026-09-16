# Resolving a code conflict by "keep both sides" can drop a shared closing brace and reorder a `mod` list; in a source-only worktree, `rustfmt --check` is the minimum gate before pushing (2026-09-08, #743's post-stack merge)

**What happened.** After the C-04/C-06 stack landed, GitHub reported #743
as conflicted. Its branch and the stack had both appended at the same
anchors: the `mod sim_cluster_*` list in `lib.rs`, the `SimCluster`
wrapper `impl` in `sim_cluster.rs`, the `animusd` guide's appendix, and the
lessons log. Every hunk was append-append with no overlapping names, so the
resolution was "keep both sides". Three pushes later the branch was green:
the first push failed to compile because the `sim_cluster.rs` hunk boundary
sat on a closing brace both sides shared (each side's last method relied on
it, so keeping both bodies left one method unclosed); the second failed
`cargo fmt --check` because placing the branch's `mod` line ahead of the
stack's put the list out of rustfmt's `reorder_modules` order; the third
failed clippy on an import the branch's side of the `use` hunk still named
while `main` had dropped its only use.

**Why it generalizes.** A conflict hunk is a text region, not a syntax
unit. When both sides end inside the same syntactic construct, the closing
delimiter after the hunk belongs to *one* of them, and "keep both"
silently steals it from the other. The same append-append shape is safe in
an append-only log and unsafe in code. And a resolution done in a
source-only worktree (no `target/`, cargo busy elsewhere) has no compiler
to catch any of this before CI does.

**Rules.** (1) In a source-only worktree, run
`rustfmt --check --edition 2024 <file>` on every hand-resolved `.rs` file
before pushing: it parses the file, needs no target directory, and catches
both the delimiter and the module-order mistakes in seconds. (2) When a
code hunk's two sides both end mid-construct, resolve by looking at the
shared line after the hunk and duplicating it between the sides if both
need it, rather than concatenating and hoping. (3) Prefer a resolution that
mirrors `main`'s ordering for anything rustfmt reorders (`mod` lists, `use`
groups), so the formatter has nothing to move. (4) When one side's `use`
line is a superset of the other's, check that every name it carries is
still used on the merged tree; `main` may have removed a use the branch
never had.
