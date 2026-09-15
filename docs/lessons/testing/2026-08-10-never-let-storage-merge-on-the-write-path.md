# Never `let _ = storage.merge(...)` on the write path

**Never `let _ = storage.merge(...)` on the write path** — an ack must mean the
write durably applied; surface storage errors so a non-durable write isn't
counted toward the quorum (`animus-data` `ack_durability.rs`).
