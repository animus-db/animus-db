# A format that lives inside engine row values needs its own harness transcode

**Context.** ADR 0073's upgrade-restart harness transcodes *whole files*
(`lsm-wal`, `control-wal`, ...) behind a per-format table. `txn-envelope` v2 (an
intent carries the committed value it shadows) is different: the envelope is the
*value* of an engine row, inside WAL records and SSTable blocks that other formats
own. The first cut of the v2 bump therefore kept its v1 encoder `cfg(test)` and
called the format "covered by its fixture", on the reasoning that nothing in the
harness rewrites row values. That left the one path a v2 bump exists to protect --
a v1 intent still unresolved across an upgrade, which has no carried prior and falls
back to the MVCC lookback -- unexercised by every restart tier.

**Lesson.** When a format cannot be reached by the harness's existing transcode,
extend the harness; do not downgrade the checklist. Concretely:

* the transcode has to *rewrite the stored bytes of every carrier* (here WAL segments
  at their existing version, and SSTables with their manifest entry, since a rewritten
  table changes its index offset, size and Bloom filter), not just provide an encoder;
* be strict about recognising the format (`downgrade_intent_to_v1` parses the whole v2
  shape and its end before touching a value): a row carries no type marker beyond the
  tag, and the same engine files hold unrelated values;
* assert the rewrite *happened* in every cell (a non-zero rewritten count and the raw
  row's tag byte before and after), and mutation-check it (a down-conversion that drops
  the staged value must fail cells): a transcode that silently rewrites nothing makes a
  green restart prove nothing, the failure mode ADR 0073 already names for an identity
  transcode;
* a completeness check ties the registration to the bump (an `EMBEDDED` format past v1
  must have a carrier transcode at that version or a `ROW_TABLE` entry), so the next
  engine-resident bump cannot repeat the shortcut;
* pin any residual the old shape leaves as a control rather than hiding it: the v1
  intent's lookback loses the value under compaction, so the cells keep the engine from
  compacting between stage and resolve and one control asserts the loss.
