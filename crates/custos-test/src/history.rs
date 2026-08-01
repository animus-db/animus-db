//! History model and recorder (Elle/Jepsen style).
//!
//! Operations are transactions of *micro-ops* over a **list-append** datatype:
//! `Append(key, value)` grows a key's list, `Read(key)` observes it. Each
//! operation is recorded as an `Invoke` followed by exactly one of `Ok`,
//! `Fail`, or `Info`.
//!
//! Crucial rule: an operation whose outcome is **indeterminate** (e.g. a
//! timeout — the write may or may not have taken effect) MUST be recorded as
//! [`Outcome::Info`], never `Fail`. `Fail` asserts the operation definitely did
//! not happen; misclassifying an indeterminate op as `Fail` makes a checker draw
//! false conclusions.

use serde::{Deserialize, Serialize};

/// A logical client/process issuing operations.
pub type Process = u64;
/// A list-append key.
pub type Key = u64;
/// An element appended to a key's list.
pub type ListVal = u64;

/// A transaction micro-op.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mop {
    /// Append `value` to `key`'s list.
    Append { key: Key, value: ListVal },
    /// Read `key`'s list. `observed` is `None` at invoke time, `Some(list)` once
    /// the operation completes.
    Read {
        key: Key,
        observed: Option<Vec<ListVal>>,
    },
}

/// The outcome class of a history entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// The operation was invoked (its effects not yet known).
    Invoke,
    /// The operation definitely completed.
    Ok,
    /// The operation definitely did not happen.
    Fail,
    /// The operation's outcome is indeterminate (e.g. a timeout).
    Info,
}

/// One entry in the history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// The process that issued the operation.
    pub process: Process,
    /// The outcome class.
    pub outcome: Outcome,
    /// Virtual-time stamp (nanoseconds).
    pub time: u64,
    /// The transaction's micro-ops.
    pub mops: Vec<Mop>,
}

/// A recorded history plus the seed that produced it (for replay).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct History {
    /// The run's seed.
    pub seed: u64,
    /// Entries in the order they were recorded.
    pub entries: Vec<Entry>,
}

impl History {
    /// The completed (`Ok`) transactions, in record order.
    pub fn ok_entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter().filter(|e| e.outcome == Outcome::Ok)
    }
}

/// Records operations into a [`History`]. Append `Invoke` first, then exactly
/// one terminal entry (`ok`/`fail`/`info`) per operation.
#[derive(Debug)]
pub struct Recorder {
    history: History,
}

impl Recorder {
    /// Create a recorder for a run identified by `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            history: History {
                seed,
                entries: Vec::new(),
            },
        }
    }

    /// Record an invocation.
    pub fn invoke(&mut self, process: Process, time: u64, mops: Vec<Mop>) {
        self.push(process, Outcome::Invoke, time, mops);
    }

    /// Record a definite completion.
    pub fn ok(&mut self, process: Process, time: u64, mops: Vec<Mop>) {
        self.push(process, Outcome::Ok, time, mops);
    }

    /// Record a definite non-occurrence (the op provably did not happen).
    pub fn fail(&mut self, process: Process, time: u64, mops: Vec<Mop>) {
        self.push(process, Outcome::Fail, time, mops);
    }

    /// Record an indeterminate outcome (use this for timeouts — never `fail`).
    pub fn info(&mut self, process: Process, time: u64, mops: Vec<Mop>) {
        self.push(process, Outcome::Info, time, mops);
    }

    /// The recorded history.
    #[must_use]
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Consume the recorder, yielding the history.
    #[must_use]
    pub fn into_history(self) -> History {
        self.history
    }

    fn push(&mut self, process: Process, outcome: Outcome, time: u64, mops: Vec<Mop>) {
        self.history.entries.push(Entry {
            process,
            outcome,
            time,
            mops,
        });
    }
}
