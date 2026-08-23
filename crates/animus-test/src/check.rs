//! Consistency checkers over a recorded [`History`].
//!
//! - [`check_cycles`] — the transactional path. It recovers each key's append
//!   order from observed reads and builds a dependency graph with write→read
//!   (`wr`), write→write (`ww`), and read→write anti-dependency (`rw`) edges; a
//!   cycle witnesses a serializability violation (Adya's G1c/G2). This is the
//!   core idea behind Elle.
//! - [`check_durability`] / [`check_convergence`] — the AP path. Durability:
//!   every acknowledged append is observed by a final quorum read. Convergence:
//!   two independent final quorum reads agree.

use std::collections::{BTreeMap, BTreeSet};

use crate::history::{History, Key, ListVal, Mop};

/// Result of a checker: a verdict plus human-readable violation descriptions and
/// the seed for replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckReport {
    /// Whether the property held.
    pub ok: bool,
    /// One description per detected violation (empty when `ok`).
    pub violations: Vec<String>,
    /// The seed of the run, for replay.
    pub seed: u64,
}

impl CheckReport {
    fn passed(seed: u64) -> Self {
        Self {
            ok: true,
            violations: Vec::new(),
            seed,
        }
    }

    fn failed(seed: u64, violations: Vec<String>) -> Self {
        Self {
            ok: false,
            violations,
            seed,
        }
    }
}

/// Recovered per-key append order, plus a map from a value to the transaction
/// that appended it.
struct Recovered {
    order: BTreeMap<Key, Vec<ListVal>>,
    appender: BTreeMap<(Key, ListVal), usize>,
    /// Reads whose observed list is not a prefix of the recovered order (a
    /// divergence anomaly).
    conflicts: Vec<String>,
}

impl Recovered {
    /// All appends to `key`, as `((key, value), appender_txn)` pairs.
    fn appends_to(&self, key: Key) -> impl Iterator<Item = (&(Key, ListVal), &usize)> {
        self.appender
            .range((key, ListVal::MIN)..=(key, ListVal::MAX))
    }
}

/// Recover append order for every key from the longest observed read of that
/// key, checking that all other observed reads are prefixes of it.
fn recover(history: &History) -> Recovered {
    let ok: Vec<&crate::history::Entry> = history.ok_entries().collect();

    let mut appender: BTreeMap<(Key, ListVal), usize> = BTreeMap::new();
    let mut longest: BTreeMap<Key, Vec<ListVal>> = BTreeMap::new();
    for (txn, entry) in ok.iter().enumerate() {
        for mop in &entry.mops {
            match mop {
                Mop::Append { key, value } => {
                    appender.insert((*key, *value), txn);
                }
                Mop::Read {
                    key,
                    observed: Some(list),
                } => {
                    if list.len() > longest.get(key).map_or(0, Vec::len) {
                        longest.insert(*key, list.clone());
                    }
                }
                Mop::Read { observed: None, .. } => {}
            }
        }
    }

    // Every observed read must be a prefix of the recovered (longest) order.
    let mut conflicts = Vec::new();
    for entry in &ok {
        for mop in &entry.mops {
            if let Mop::Read {
                key,
                observed: Some(list),
            } = mop
            {
                let order = longest.get(key).cloned().unwrap_or_default();
                if !order.starts_with(list) {
                    conflicts.push(format!(
                        "divergent read of key {key}: observed {list:?} is not a prefix of recovered order {order:?}"
                    ));
                }
            }
        }
    }

    Recovered {
        order: longest,
        appender,
        conflicts,
    }
}

/// Per-`Ok`-transaction real-time span `[invoke, complete]`, indexed exactly as
/// [`check_cycles`] indexes transactions (position within
/// [`History::ok_entries`]).
///
/// A process issues operations serially, so pairing is a single forward walk:
/// an `Invoke` stamps that process's pending start, and the terminal entry
/// consumes it. A terminal `Fail`/`Info` clears the pending start without
/// producing a span (those transactions are not graph vertices).
///
/// If a well-formed pair is ever missing an `Invoke`, the span starts at `0` —
/// deliberately the **conservative** fallback: an op that appears to have begun
/// at the start of the run has nothing preceding it, so the unpaired entry can
/// only ever *lose* real-time edges, never gain a spurious one.
fn realtime_spans(history: &History) -> Vec<(u64, u64)> {
    use crate::history::{Outcome, Process};

    let mut pending: BTreeMap<Process, u64> = BTreeMap::new();
    let mut spans = Vec::new();
    for entry in &history.entries {
        match entry.outcome {
            Outcome::Invoke => {
                pending.insert(entry.process, entry.time);
            }
            Outcome::Ok => {
                let start = pending.remove(&entry.process).unwrap_or(0);
                spans.push((start, entry.time));
            }
            Outcome::Fail | Outcome::Info => {
                pending.remove(&entry.process);
            }
        }
    }
    spans
}

/// How many **real-time** precedence edges [`check_strict_cycles`] contributes
/// over [`check_cycles`] for this history.
///
/// A corpus uses this as a **non-vacuity guard**: the strict check only has
/// teeth where operations genuinely do not overlap in real time, so a scenario
/// reporting zero here proves nothing stronger than plain serializability, no
/// matter how green it is.
#[must_use]
pub fn realtime_edge_count(history: &History) -> usize {
    let spans = realtime_spans(history);
    let mut n = 0;
    for (a, &(_, a_done)) in spans.iter().enumerate() {
        for (b, &(b_start, _)) in spans.iter().enumerate() {
            if a != b && a_done < b_start {
                n += 1;
            }
        }
    }
    n
}

/// Check the transactional path for **serializability** cycles.
///
/// Data dependencies only (`ww`/`wr`/`rw`): this proves the history is
/// *equivalent to some serial order*, and deliberately says nothing about
/// whether that order respects real time. For a plane that claims
/// **linearizable** reads, use [`check_strict_cycles`] instead — see its doc
/// for exactly what this one cannot see.
pub fn check_cycles(history: &History) -> CheckReport {
    cycles_inner(history, false)
}

/// Check the transactional path for **strict**-serializability cycles: the
/// [`check_cycles`] dependency graph *plus* real-time precedence edges.
///
/// For single-object operations strict serializability is linearizability, so
/// this is the checker a linearizable plane (ADR 0016/0017) must be held to.
///
/// **Why the plain check is not enough.** With a one-mop-per-transaction
/// workload (the shape `raftkv_linearizable` drives), a read-only transaction
/// can never lie on a data-dependency cycle: its only outgoing edges are `rw`
/// to appenders it *missed*, its only incoming edges are `wr` from appenders it
/// *saw*, the sole append→append edges (`ww`) run strictly forward through the
/// recovered order, and every read surviving [`recover`]'s prefix check saw a
/// prefix — so everything it missed sorts after everything it saw and no path
/// leads back. A read served from stale state therefore produces a history that
/// is perfectly serializable (order the stale read earlier) while being flatly
/// non-linearizable. That is precisely the deposed-leader stale read ADR 0017
/// §3's ReadIndex barrier exists to prevent, and the reason this variant exists.
///
/// An edge runs `A → B` when `A` **completed** before `B` was **invoked**;
/// operations that overlap in real time are unordered by it, exactly as
/// linearizability allows. [`realtime_edge_count`] reports how many such edges a
/// history actually admits, so a caller can assert the check is not vacuous.
pub fn check_strict_cycles(history: &History) -> CheckReport {
    cycles_inner(history, true)
}

/// The shared body of [`check_cycles`] and [`check_strict_cycles`].
fn cycles_inner(history: &History, strict: bool) -> CheckReport {
    let rec = recover(history);

    let ok: Vec<&crate::history::Entry> = history.ok_entries().collect();
    let mut edges: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    let edge = |from: usize, to: usize, edges: &mut BTreeMap<usize, BTreeSet<usize>>| {
        if from != to {
            edges.entry(from).or_default().insert(to);
        }
    };

    // ww: consecutive appenders in each key's recovered order.
    for (key, order) in &rec.order {
        for pair in order.windows(2) {
            if let (Some(&a), Some(&b)) = (
                rec.appender.get(&(*key, pair[0])),
                rec.appender.get(&(*key, pair[1])),
            ) {
                edge(a, b, &mut edges);
            }
        }
    }

    // wr and rw, from each read.
    for (txn, entry) in ok.iter().enumerate() {
        for mop in &entry.mops {
            if let Mop::Read {
                key,
                observed: Some(list),
            } = mop
            {
                let seen: BTreeSet<ListVal> = list.iter().copied().collect();
                // wr: every observed value's writer happens-before this reader.
                for v in list {
                    if let Some(&w) = rec.appender.get(&(*key, *v)) {
                        edge(w, txn, &mut edges);
                    }
                }
                // rw: this read did not observe these appends, so it precedes
                // their writers (it read an earlier state of the list).
                for (&(_, value), &w) in rec.appends_to(*key) {
                    if !seen.contains(&value) {
                        edge(txn, w, &mut edges);
                    }
                }
            }
        }
    }

    // Real-time precedence (strict serializability only): `A → B` whenever `A`
    // completed before `B` was invoked. Overlapping operations stay unordered.
    if strict {
        let spans = realtime_spans(history);
        debug_assert_eq!(
            spans.len(),
            ok.len(),
            "real-time spans must index identically to `ok_entries`"
        );
        for (a, &(_, a_done)) in spans.iter().enumerate() {
            for (b, &(b_start, _)) in spans.iter().enumerate() {
                if a_done < b_start {
                    edge(a, b, &mut edges);
                }
            }
        }
    }

    let mut violations = rec.conflicts;
    for scc in strongly_connected(&edges, ok.len()) {
        if scc.len() > 1 {
            violations.push(format!("dependency cycle among transactions {scc:?}"));
        }
    }

    if violations.is_empty() {
        CheckReport::passed(history.seed)
    } else {
        CheckReport::failed(history.seed, violations)
    }
}

/// Durability: every acknowledged append is present in `final_lists` (a final
/// quorum read). A missing value is a lost acknowledged write.
pub fn check_durability(
    history: &History,
    final_lists: &BTreeMap<Key, Vec<ListVal>>,
) -> CheckReport {
    let mut violations = Vec::new();
    for entry in history.ok_entries() {
        for mop in &entry.mops {
            if let Mop::Append { key, value } = mop {
                let present = final_lists.get(key).is_some_and(|l| l.contains(value));
                if !present {
                    violations.push(format!(
                        "lost acknowledged append: value {value} to key {key} (recorded ok at t={}) absent from final state",
                        entry.time
                    ));
                }
            }
        }
    }
    if violations.is_empty() {
        CheckReport::passed(history.seed)
    } else {
        CheckReport::failed(history.seed, violations)
    }
}

/// Convergence: two independent final quorum reads observe the same state.
pub fn check_convergence(
    seed: u64,
    read_a: &BTreeMap<Key, Vec<ListVal>>,
    read_b: &BTreeMap<Key, Vec<ListVal>>,
) -> CheckReport {
    let mut violations = Vec::new();
    let keys: BTreeSet<Key> = read_a.keys().chain(read_b.keys()).copied().collect();
    for key in keys {
        let a = read_a.get(&key);
        let b = read_b.get(&key);
        if a != b {
            violations.push(format!("non-convergent key {key}: {a:?} vs {b:?}"));
        }
    }
    if violations.is_empty() {
        CheckReport::passed(seed)
    } else {
        CheckReport::failed(seed, violations)
    }
}

/// Tarjan's strongly-connected components over `0..n` with adjacency `edges`.
/// An SCC with more than one node (or a self-loop, excluded at edge-build time)
/// indicates a cycle.
fn strongly_connected(edges: &BTreeMap<usize, BTreeSet<usize>>, n: usize) -> Vec<Vec<usize>> {
    struct Tarjan<'a> {
        edges: &'a BTreeMap<usize, BTreeSet<usize>>,
        index: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        next: usize,
        out: Vec<Vec<usize>>,
    }
    impl Tarjan<'_> {
        fn dfs(&mut self, v: usize) {
            self.index[v] = Some(self.next);
            self.low[v] = self.next;
            self.next += 1;
            self.stack.push(v);
            self.on_stack[v] = true;
            if let Some(succ) = self.edges.get(&v) {
                for &w in succ {
                    match self.index[w] {
                        None => {
                            self.dfs(w);
                            self.low[v] = self.low[v].min(self.low[w]);
                        }
                        Some(idx) if self.on_stack[w] => {
                            self.low[v] = self.low[v].min(idx);
                        }
                        Some(_) => {}
                    }
                }
            }
            if self.index[v] == Some(self.low[v]) {
                let mut comp = Vec::new();
                loop {
                    let w = self.stack.pop().expect("non-empty SCC stack");
                    self.on_stack[w] = false;
                    comp.push(w);
                    if w == v {
                        break;
                    }
                }
                comp.sort_unstable();
                self.out.push(comp);
            }
        }
    }

    let mut t = Tarjan {
        edges,
        index: vec![None; n],
        low: vec![0; n],
        on_stack: vec![false; n],
        stack: Vec::new(),
        next: 0,
        out: Vec::new(),
    };
    for v in 0..n {
        if t.index[v].is_none() {
            t.dfs(v);
        }
    }
    t.out
}
