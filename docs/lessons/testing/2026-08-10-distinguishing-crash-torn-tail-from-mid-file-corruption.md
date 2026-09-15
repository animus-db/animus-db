# Distinguishing "crash-torn tail" from "mid-file corruption" needs a positional proof, not a magnitude heuristic — scan forward for the next valid checksummed frame; if one exists, the failure is real corruption.

**Distinguishing "crash-torn tail" from "mid-file corruption" needs a
positional proof, not a magnitude heuristic — scan forward for the next
valid checksummed frame; if one exists, the failure is real corruption.**
A torn-and-happens-to-look-corrupted tail and genuine mid-file corruption
can produce equally implausible declared lengths, so "does the length look
sane" can't tell them apart. The WAL's binary frame decoder resolves a
parse failure by resyncing forward: tolerate it as a crash-torn tail only
if NO later valid frame is found in the buffer; otherwise it's a hard
error. (`wal_resync_point`, PR #32.)
