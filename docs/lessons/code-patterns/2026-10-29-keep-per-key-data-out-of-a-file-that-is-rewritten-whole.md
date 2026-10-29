# Per-key data does not belong in a file that is rewritten whole on every change

The LSM `MANIFEST` is published with `Disk::replace` (write temp, fsync, rename,
fsync dir) on every flush and compaction. It carried every live table's Bloom
bits inline, so each flush rewrote and fsynced a file that grew with the data
(149 KB in a soak, issue #1389). It was never "history accumulating": the
existing plateau test over 8 keys passed because tiny filters hid the cost.

Rules that came out of it:

- A metadata file that is replaced whole must cost a small constant per object,
  independent of the object's size. Put the per-key payload (filters, indexes)
  in the object's own file, written and synced before the manifest references it.
- A plateau test must use inputs whose payload is real. Assert a per-object byte
  budget (here 256 B per live table) with many distinct keys per object, and
  measure the old behaviour first (565 B per table) so the test provably fails on
  the design it guards against. Do not assert plateau against total size when live
  object count legitimately grows.
- When the new layout only appends a region after the old one (blocks, index,
  then filter, then the same footer), old readers that take extents from the
  manifest still read the file. That makes the harness's transcode back to the
  old version honest and the identity, instead of inventing mixed states.
- A "not loaded yet" marker (`has_bloom == false` on a decoded format-2 meta) must
  be adopted back into the engine's in-memory manifest after open, or introspection
  and any re-encode path see the unloaded copy.
