//! Export a [`History`] for offline analysis.
//!
//! [`to_json`] emits the native representation; [`to_edn`] emits Jepsen/Elle
//! style EDN lines (`{:process .. :type .. :f :txn :value [[:append k v] ..]
//! :time ..}`) so a history can be fed to the real Elle/Jepsen tooling later.

use crate::history::{Entry, History, Mop, Outcome};

/// Serialize the history to pretty JSON.
///
/// # Panics
/// Never in practice: the history is composed of plain serializable types.
#[must_use]
pub fn to_json(history: &History) -> String {
    serde_json::to_string_pretty(history).expect("history serializes to json")
}

/// Render the history as Jepsen/Elle-style EDN, one operation per line.
#[must_use]
pub fn to_edn(history: &History) -> String {
    history
        .entries
        .iter()
        .map(edn_entry)
        .collect::<Vec<_>>()
        .join("\n")
}

fn edn_entry(entry: &Entry) -> String {
    let type_kw = match entry.outcome {
        Outcome::Invoke => ":invoke",
        Outcome::Ok => ":ok",
        Outcome::Fail => ":fail",
        Outcome::Info => ":info",
    };
    let value = entry.mops.iter().map(edn_mop).collect::<Vec<_>>().join(" ");
    format!(
        "{{:process {}, :type {}, :f :txn, :value [{}], :time {}}}",
        entry.process, type_kw, value, entry.time
    )
}

fn edn_mop(mop: &Mop) -> String {
    match mop {
        Mop::Append { key, value } => format!("[:append {key} {value}]"),
        Mop::Read { key, observed } => match observed {
            None => format!("[:r {key} nil]"),
            Some(list) => {
                let elems = list
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("[:r {key} [{elems}]]")
            }
        },
    }
}
