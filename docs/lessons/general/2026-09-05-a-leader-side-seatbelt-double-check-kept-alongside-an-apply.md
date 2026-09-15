# A leader-side "seatbelt double-check" kept alongside an apply-evaluated write must predict the client's own decision, not replicate the byte-level mechanism it replaces — superseded, moved to the archive

The mechanism this entry described
(`predict_kind_eval_decision`/`report_kind_eval_seatbelt_mismatch`,
`Metric::KindEvalSeatbeltMismatch`, `rmw285_confirm_gate`) was deleted whole by
ADR 0054 step 4b — see `docs/engineering-lessons-archive.md`'s matching entry
for the full account. The lesson still generalizes: a legacy double-check kept
alongside a cutover must predict the SAME decision the new path makes, not
replicate the old mechanism's own byte-level signal.
