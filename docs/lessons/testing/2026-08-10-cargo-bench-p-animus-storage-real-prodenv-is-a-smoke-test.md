# `cargo bench -p animus-storage` (real `ProdEnv`) is a smoke test the deterministic suite is not

**`cargo bench -p animus-storage` (real `ProdEnv`) is a smoke test the
deterministic suite is not** — it surfaced that same deadlock. Run it when
touching the write/IO path.
