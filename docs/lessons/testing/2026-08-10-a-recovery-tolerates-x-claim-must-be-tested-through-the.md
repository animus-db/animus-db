# A "recovery tolerates X" claim must be tested through the NEXT write cycle, not just one reopen.

**A "recovery tolerates X" claim must be tested through the NEXT write cycle,
not just one reopen.** The LSM tolerated a torn WAL tail on replay (skipped the
torn line) but reused the un-truncated active segment, so the next acked record
was appended after garbage and a SECOND restart silently dropped it — the
crash-recovery instance of the "prove recursive ops at depth ≥ 2" rule: recover,
write, recover again, then assert. (PR #24's fault injection; fix = seal the
recovered segment.)
