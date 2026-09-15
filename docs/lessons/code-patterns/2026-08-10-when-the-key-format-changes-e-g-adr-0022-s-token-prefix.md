# When the key format changes (e.g. ADR 0022's token prefix), sweep *every* key-building write path, not just the wire edges — a path that bypasses the shared layout partitions a different keyspace.

**When the key format changes (e.g. ADR 0022's token prefix), sweep *every*
key-building write path, not just the wire edges — a path that bypasses the shared
layout partitions a different keyspace.** The admin bulk-seed endpoint kept writing
raw `prefix+index` bytes via `cp_write` after the DynamoDB/CQL builders gained the
Murmur3 token prefix, so seeded tables split at raw-key medians (readable ranges in
the dashboard — the visible symptom), sequential seeds piled into one tablet's tail
(the exact skew the token removes), and a mixed seed+edge table would interleave two
key layouts in one engine. `cp_write`/`cp_read` take the key verbatim, so nothing
below the edge catches this — grep for `cp_write` callers and check each builds the
ADR 0022 layout. (`animusd` `admin.rs::seed_key`.)
