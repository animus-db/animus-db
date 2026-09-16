# The `debug = 0` cargo-config fix for this workspace's disk pressure is worth applying *before* the first ENOSPC, not after the fourth.

**The `debug = 0` cargo-config fix for this workspace's disk pressure is
worth applying *before* the first ENOSPC, not after the fourth.** The
entry above describes it as the remedy once you are already wedged; in
practice a session that builds `animusd --all-targets` more than a couple
of times will get there, because each rebuild of the ~90 integration test
binaries re-links the same large dependency set with full debuginfo.
Measured in one session: writing `[profile.dev]`/`[profile.test]`
`debug = 0`, `incremental = false` to `/root/.cargo/config.toml` and
re-running took free space from **3.6 GB to 18 GB** and left the full
build passing in 1m21s. Confirm it took by the build summary line —
`unoptimized` with no `+ debuginfo`. Nothing is lost that matters here:
these are integration tests asserting on HTTP responses, not something
anyone attaches a debugger to. The cost of *not* doing it is worse than
lost disk — an ENOSPC surfaces as a linker `cc` failure or a garbled
compile error, so it reads as a code problem and costs a diagnosis
before it costs a cleanup.
