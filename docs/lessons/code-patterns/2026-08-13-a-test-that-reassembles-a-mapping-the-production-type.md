# A test that reassembles a mapping the production type already owns is a second, silently-diverging copy of the spec.

**A test that reassembles a mapping the production type already owns is a
second, silently-diverging copy of the spec.** `StorageScope` maps a logical
key to its physical engine key (`prefix || key`), and `StorageScope::whole()`
was documented as "every physical-key operation is an identity transform."
Seven test files had quietly baked that in — reading `node.storage().get(k)`
with a *logical* `k`, or building `physical()` as `prefix_for(TABLE) || key` —
so they were asserting against a hand-copied duplicate of the mapping rather
than the mapping itself. When ADR 0041 §3 inserted a row-kind byte
(`prefix || kind || key`, making `whole()` no longer the identity), every one
of those copies became wrong at once. **This time the divergence was loud** —
the reads returned `None` and the tests failed — but that was luck, not
design: a mapping change that made a test read the *wrong existing key*
instead of a missing one would have failed silently or, worse, passed. **The
practice**: when a type owns a logical→physical (or encode→store) mapping,
give callers a public accessor for it (here `RaftKvNode::physical_key(kind,
key)`) and let nothing outside reimplement it — a test that needs the physical
key should *ask*, and a test with no handle to ask through should at least
name the layout element explicitly rather than inheriting it by omission.
Corollary for the doc: "X is the identity transform" invites callers to skip
the mapping entirely, which is the most brittle form of this duplication —
prefer documenting *that a mapping exists* over documenting that it currently
happens to be free.
