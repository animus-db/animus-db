//! The node-wide **memtable budget** that sizes each engine's flush threshold
//! (issue #1389, ADR 0079 amendment).
//!
//! A fixed per-tablet flush threshold cannot serve both ends of the density
//! range: a tiny one (64 KiB) makes a hot tablet flush a swarm of tiny L0
//! tables and rewrite them over and over (high compaction bytes per commit),
//! while a large one (2 MiB) times thousands of hosted tablets blows the node's
//! memory. So the threshold is derived from one shared budget:
//!
//! ```text
//! threshold = clamp(budget / active, floor, cap)
//! ```
//!
//! where `active` is the number of engines on the node that currently hold
//! unflushed memtable bytes. A lone hot tablet gets `cap`; as more tablets
//! write, each one's share shrinks toward `floor`; a tablet that flushes (or
//! is quiesced/released and so empties its memtable, or is dropped) stops
//! counting. While `budget / active >= floor` every engine flushes at or below
//! its share, so the node's total memtable bytes stay within `budget` (modulo
//! the transient where an idle engine holds bytes sized for an older, smaller
//! `active`). Past `budget / floor` active engines the floor binds and the
//! total can exceed the budget by design: the floor is what keeps flushes from
//! degenerating into tiny tables.
//!
//! The object is a cheap `Arc` of atomics: reading the threshold is a couple of
//! relaxed loads and no lock, no clock and no entropy, so it is deterministic
//! under `SimEnv`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Default per-tablet flush floor: the old fixed threshold.
pub const DEFAULT_FLUSH_FLOOR_BYTES: usize = 64 * 1024;
/// Default per-tablet flush cap.
pub const DEFAULT_FLUSH_CAP_BYTES: usize = 4 << 20;
/// Default node memtable budget (`--memtable-budget-bytes`).
pub const DEFAULT_NODE_MEMTABLE_BUDGET_BYTES: usize = 256 << 20;

struct Shared {
    budget: AtomicUsize,
    floor: usize,
    cap: usize,
    active: AtomicUsize,
}

/// The shared per-node memtable budget. Clones share state.
#[derive(Clone)]
pub struct FlushBudget {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for FlushBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushBudget")
            .field("budget", &self.budget())
            .field("floor", &self.shared.floor)
            .field("cap", &self.shared.cap)
            .field("active", &self.active())
            .finish()
    }
}

impl FlushBudget {
    /// A budget of `budget_bytes` shared by the node's engines, each getting a
    /// threshold in `[floor, cap]` (`cap` is raised to `floor` if smaller; a
    /// zero `floor` is treated as 1).
    #[must_use]
    pub fn new(budget_bytes: usize, floor: usize, cap: usize) -> Self {
        let floor = floor.max(1);
        Self {
            shared: Arc::new(Shared {
                budget: AtomicUsize::new(budget_bytes),
                floor,
                cap: cap.max(floor),
                active: AtomicUsize::new(0),
            }),
        }
    }

    /// The production defaults: 256 MiB budget, 64 KiB floor, 4 MiB cap.
    #[must_use]
    pub fn production(budget_bytes: usize) -> Self {
        Self::new(
            budget_bytes,
            DEFAULT_FLUSH_FLOOR_BYTES,
            DEFAULT_FLUSH_CAP_BYTES,
        )
    }

    /// The pure split: `clamp(budget / max(active, 1), floor, cap)`.
    #[must_use]
    pub fn split(budget: usize, active: usize, floor: usize, cap: usize) -> usize {
        (budget / active.max(1)).clamp(floor, cap.max(floor))
    }

    /// The flush threshold an engine should use right now.
    #[must_use]
    pub fn threshold(&self) -> usize {
        Self::split(
            self.budget(),
            self.active(),
            self.shared.floor,
            self.shared.cap,
        )
    }

    /// The configured node budget (bytes).
    #[must_use]
    pub fn budget(&self) -> usize {
        self.shared.budget.load(Ordering::Relaxed)
    }

    /// Change the node budget at runtime.
    pub fn set_budget(&self, bytes: usize) {
        self.shared.budget.store(bytes, Ordering::Relaxed);
    }

    /// The per-engine floor.
    #[must_use]
    pub fn floor(&self) -> usize {
        self.shared.floor
    }

    /// The per-engine cap.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.shared.cap
    }

    /// Engines currently holding unflushed memtable bytes.
    #[must_use]
    pub fn active(&self) -> usize {
        self.shared.active.load(Ordering::Relaxed)
    }

    /// A new engine slot (inactive until it reports a non-empty memtable).
    #[must_use]
    pub fn slot(&self) -> FlushSlot {
        FlushSlot {
            budget: self.clone(),
            dirty: AtomicBool::new(false),
        }
    }
}

/// One engine's registration in a [`FlushBudget`]; counts as active while its
/// memtable is non-empty, and stops counting when dropped.
pub struct FlushSlot {
    budget: FlushBudget,
    dirty: AtomicBool,
}

impl FlushSlot {
    /// Report whether the engine's memtable currently holds bytes.
    pub fn set_dirty(&self, dirty: bool) {
        if self.dirty.swap(dirty, Ordering::AcqRel) != dirty {
            if dirty {
                self.budget.shared.active.fetch_add(1, Ordering::Relaxed);
            } else {
                self.budget.shared.active.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// The budget this slot belongs to.
    #[must_use]
    pub fn budget(&self) -> &FlushBudget {
        &self.budget
    }
}

impl Drop for FlushSlot {
    fn drop(&mut self) {
        self.set_dirty(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_divides_budget_and_clamps() {
        let (floor, cap) = (64 << 10, 4 << 20);
        let b = 256 << 20;
        assert_eq!(FlushBudget::split(b, 0, floor, cap), cap);
        assert_eq!(FlushBudget::split(b, 1, floor, cap), cap);
        assert_eq!(FlushBudget::split(b, 64, floor, cap), cap);
        assert_eq!(FlushBudget::split(b, 128, floor, cap), 2 << 20);
        assert_eq!(FlushBudget::split(b, 1024, floor, cap), 256 << 10);
        assert_eq!(FlushBudget::split(b, 4096, floor, cap), floor);
        assert_eq!(FlushBudget::split(b, 100_000, floor, cap), floor);
    }

    #[test]
    fn degenerate_bounds() {
        let b = FlushBudget::new(0, 100, 10);
        assert_eq!(b.floor(), 100);
        assert_eq!(b.cap(), 100);
        assert_eq!(b.threshold(), 100);
        assert_eq!(FlushBudget::new(1 << 20, 0, 10).floor(), 1);
    }

    #[test]
    fn threshold_follows_active_engines_and_budget() {
        let b = FlushBudget::new(1 << 20, 1 << 10, 256 << 10);
        let slots: Vec<FlushSlot> = (0..8).map(|_| b.slot()).collect();
        assert_eq!(b.active(), 0);
        assert_eq!(b.threshold(), 256 << 10);
        slots[0].set_dirty(true);
        slots[0].set_dirty(true); // idempotent
        assert_eq!(b.active(), 1);
        for s in &slots {
            s.set_dirty(true);
        }
        assert_eq!(b.active(), 8);
        assert_eq!(b.threshold(), (1 << 20) / 8);
        slots[3].set_dirty(false);
        assert_eq!(b.threshold(), (1 << 20) / 7);
        b.set_budget(8 << 10);
        assert_eq!(b.threshold(), (8 << 10) / 7);
        b.set_budget(4 << 10);
        assert_eq!(b.threshold(), 1 << 10);
        b.set_budget(8 << 10);
        drop(slots);
        assert_eq!(b.active(), 0);
        assert_eq!(b.threshold(), 8 << 10);
    }
}
