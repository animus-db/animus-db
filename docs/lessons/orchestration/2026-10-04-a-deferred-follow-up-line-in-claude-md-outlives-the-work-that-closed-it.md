# A "deferred follow-up" line in root CLAUDE.md outlives the work that closed it

Root `CLAUDE.md` still said an S3 `SegmentStore` backend and S3 export/import
were "deferred follow-ups" weeks after S-04 and S-05 landed (`animus-s3`,
`S3SegmentStore`, `--backup-store s3://...`, the operator's `spec.s3`, ADR
0068). A request to roadmap "the S3 SegmentStore backend" would have produced
a plan to rebuild a finished feature. Landing PRs updated the ADR, the crate
guides and the roadmap section, but nobody owns the one-line summary in the
thin entry file.

**Rule:** before writing a roadmap entry for a "deferred" feature, grep the
code for the trait impl, the CLI flag and the operator field, and read the
crate guide's own history. If it has landed, the deliverable is the prose fix
plus an entry for only the real residual (here: static-only credentials, no
multipart, retry sleep outside the `Env` seam). When a PR closes a roadmap
item, also grep root `CLAUDE.md` and `website/` for its name.
