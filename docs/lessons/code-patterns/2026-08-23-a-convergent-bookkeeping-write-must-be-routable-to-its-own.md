# A convergent bookkeeping write must be routable to its own owner: derive its key from the owner's actual scope, not a normalized/truncated form of it.

**A convergent bookkeeping write must be routable to its own owner: derive
its key from the owner's actual scope, not a normalized/truncated form of
it.** Issue #355's root cause (see the Testing entry above for the full
incident): `cursor::cursor_key` derived a cursor row's key by truncating
the writing tablet's own `range.start` to a fixed-width token, on the
assumption that "this tablet's own token" was a safe stand-in for "a key
inside this tablet's own declared range." That assumption held only by
coincidence — a hash-ring token happens to be a real byte prefix of a
tablet's range only when the range boundary is itself token-aligned, which
a real split boundary (chosen from row content) essentially never is. Any
write that reaches a codebase's own key-based routing layer (here,
`ClientCtx::cp_kind_write_raw`'s `cp_route`, which resolves a target purely
from the write's own key bytes against each candidate's declared range)
needs a key that is *actually, structurally* inside its intended owner's
range — not merely "derived from" that owner in some way that seems close
enough. The fix embeds the owner's real range boundary verbatim (with a
trailing length so a fixed-width parse can still recover what follows it)
instead of a lossy summary of it. The general check: before writing a
bookkeeping/cursor/marker key that will be routed by content rather than
written directly to an already-resolved handle, ask whether the key is
provably inside the target's own bounds by construction, or only usually
inside them for shapes a test happened to try.
