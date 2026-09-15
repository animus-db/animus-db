# Never hold a `std::sync::Mutex` guard across an `.await`

**Never hold a `std::sync::Mutex` guard across an `.await`** in `<E: Env>`
code — it breaks `Send` (often a *compile* error via `spawn_task`'s bound) and
risks nondeterminism. Take the lock, mutate, drop it; do I/O lock-free.
