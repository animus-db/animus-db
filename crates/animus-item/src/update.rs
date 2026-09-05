//! The `UpdateExpression` data model and its apply-time evaluator (ADR 0054
//! step 1 — moved here from `animus-dynamo::wire` so a future apply-path
//! evaluator does not need the wire crate).
//!
//! **What moved and what did not.** The *tokenizer/parser* that turns a
//! DynamoDB `UpdateExpression` request string into a `Vec<`[`UpdateAction`]`>`
//! stays in `animus-dynamo::wire` (`decode_update_expression` and friends):
//! it resolves `#name`/`:value` placeholders against the request's own
//! `ExpressionAttributeNames`/`ExpressionAttributeValues` JSON objects, which
//! is genuinely wire/JSON-decode work, not part of the pure item-mutation
//! model. What moves here is everything **evaluation** needs once parsing has
//! already produced a fully-resolved `Vec<UpdateAction>`: the data types
//! themselves ([`PathSegment`]/[`UpdateOperand`]/[`UpdateExpr`]/
//! [`UpdateAction`], which already carry `Serialize`/`Deserialize` since they
//! ride inside a replicated write command), and the evaluator
//! ([`apply_update`] and everything it calls). That is exactly the boundary
//! ADR 0054's "evaluation moves below the wire adapter" mechanism draws.

use serde::{Deserialize, Serialize};

use crate::condition::{add_numeric, negate_numeric};
use crate::size::{MAX_ITEM_SIZE_BYTES, item_size};
use crate::{AttributeValue, Item};

/// One segment of a document path: either a map key (a plain attribute name,
/// one `.`-separated component) or a list index (one `[n]` suffix). A dotted
/// path `a.b` is two `Field` segments; a list-index path `a[0].b` is
/// `Field("a")`, `Index(0)`, `Field("b")`.
///
/// `Serialize`/`Deserialize`: an [`UpdateAction`]'s own target/operand paths
/// ride the wire inside a replicated write command the same way
/// `UpdateAction` itself does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathSegment {
    /// A map (`M`) key.
    Field(String),
    /// A list (`L`) index — zero-based, matching DynamoDB.
    Index(usize),
}

/// A `SET` clause's right-hand-side operand: an already-resolved `:value`, a
/// document path read from the item at apply time, or a function call.
///
/// **A documented simplification of DynamoDB's own within-one-expression
/// ordering semantics, not a modeled property**: a `Path` operand naming an
/// attribute a prior action in the same expression already set sees that
/// action's result (evaluation folds through the item as it is built so
/// far), which is not necessarily identical to AWS's own stricter ordering
/// rules for a single expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateOperand {
    /// An already-resolved `:value`.
    Value(AttributeValue),
    /// A document path, read from the item at apply time.
    Path(Vec<PathSegment>),
    /// `if_not_exists(path, default)` — `path`'s current value if present,
    /// else `default` (itself evaluated, so it may be another function call
    /// or a `:value`). Evaluating to nothing (a `default` that is itself an
    /// absent path) is a validation error — `SET` can never assign "no
    /// value".
    IfNotExists(Vec<PathSegment>, Box<UpdateOperand>),
    /// `list_append(a, b)` — the concatenation `a ++ b`; both operands must
    /// evaluate to a list (`L`). A missing operand, or a present one that
    /// isn't a list, is a validation error.
    ListAppend(Box<UpdateOperand>, Box<UpdateOperand>),
}

/// A `SET` clause's right-hand side: one [`UpdateOperand`], or `operand +
/// operand` / `operand - operand` — DynamoDB allows at most one arithmetic
/// operator per `SET` value, both sides numeric (`N`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateExpr {
    /// A single operand, no arithmetic.
    Operand(UpdateOperand),
    /// `lhs + rhs`, both operands numeric.
    Add(UpdateOperand, UpdateOperand),
    /// `lhs - rhs`, both operands numeric.
    Sub(UpdateOperand, UpdateOperand),
}

impl UpdateExpr {
    /// A plain `:value`/path/function-call `SET` expression, no arithmetic —
    /// convenience wrapper around [`UpdateOperand::Value`]/`Self::Operand`
    /// used by every caller that just needs "SET this literal value" (the
    /// pre-arithmetic common case).
    #[must_use]
    pub fn value(v: AttributeValue) -> Self {
        UpdateExpr::Operand(UpdateOperand::Value(v))
    }
}

/// One action of an `UpdateItem` `UpdateExpression` (the supported subset):
/// set a document path to a value/path/function-call/arithmetic expression,
/// remove one, or apply `ADD`/`DELETE`'s typed operation to one. Every
/// target is a **document path** (`Vec<`[`PathSegment`]`>`) — `a`, `a.b`,
/// `a[0]`, `#n.b[1]` — so `SET`/`REMOVE`/`ADD`/`DELETE` can all target a
/// nested map/list element, not only a top-level attribute.
///
/// **`Serialize`/`Deserialize` (ADR 0046 U3)**: rides the wire inside
/// `ClientRequest::KindWriteItem`'s `KindWriteOp::Update` — the leader-side
/// write evaluator applies `UpdateItem`'s own raw actions to the old image
/// it itself reads, rather than trusting a pre-computed new item from the
/// (possibly stale, possibly racing) edge that received the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateAction {
    /// `SET path = expr` — set (or overwrite) a document path to the result
    /// of evaluating `expr` ([`UpdateExpr`]: a value, a path,
    /// `if_not_exists(..)`/`list_append(..)`, or `operand +/- operand`)
    /// against the item at apply time. `path`'s parent must already exist
    /// (`SET a.b = :v` on an absent `a` is a validation error) — only the
    /// final segment may be new.
    Set(Vec<PathSegment>, UpdateExpr),
    /// `REMOVE path` — drop a document path if present; a no-op if any part
    /// of it is absent. `REMOVE a[i]` compacts the list.
    Remove(Vec<PathSegment>),
    /// `ADD path :v` — numeric addition when both sides are `N`, set union
    /// when both are the same set type. On an absent path it seeds the
    /// value (the same parent-must-exist rule as `SET`), which is what makes
    /// `ADD` the idiomatic counter increment.
    Add(Vec<PathSegment>, AttributeValue),
    /// `DELETE path :v` — remove `:v`'s members from a set-typed path. Only
    /// the set types; an empty result removes the attribute entirely, as
    /// DynamoDB does not store empty sets.
    Delete(Vec<PathSegment>, AttributeValue),
}

/// Render a document path back to `a.b[0]`-style text for an error message.
#[must_use]
pub fn format_update_path(path: &[PathSegment]) -> String {
    let mut out = String::new();
    for (i, seg) in path.iter().enumerate() {
        match seg {
            PathSegment::Field(name) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(name);
            }
            PathSegment::Index(n) => {
                out.push('[');
                out.push_str(&n.to_string());
                out.push(']');
            }
        }
    }
    out
}

/// Why [`apply_update`] could not apply an `UpdateExpression` — always a
/// DynamoDB `ValidationException` in practice today (the caller
/// (`animus_dynamo::wire::apply_update`) maps `code`/`message` straight onto
/// its own `WireError`). Carries its own `code` field, rather than being a
/// bare message, so that mapping — and any test asserting on it — does not
/// have to hardcode an assumption about which DynamoDB exception every
/// apply-time failure is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateError {
    /// The DynamoDB error code this failure maps to.
    pub code: &'static str,
    /// A human-readable message.
    pub message: String,
}

impl UpdateError {
    #[must_use]
    fn validation(message: impl Into<String>) -> Self {
        Self {
            code: "ValidationException",
            message: message.into(),
        }
    }
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for UpdateError {}

type Result<T> = std::result::Result<T, UpdateError>;

/// Apply `actions` to `item` in order, folding each result into the next —
/// the apply-time evaluator every `UpdateItem`/`TransactWriteItems` `Update`
/// action routes through (ADR 0054's "evaluation moves below the wire
/// adapter" — this is the function a tablet's apply path calls once it can
/// read the current item in commit order, so no stale before-image is ever
/// possible). Re-checks [`MAX_ITEM_SIZE_BYTES`] against the fold's *final*
/// result, once, after every action has applied — not mid-fold, since an
/// action that temporarily pushes the item over the cap is legal as long as
/// a later action in the same expression nets it back under.
///
/// # Errors
/// A malformed target (e.g. `SET a.b = :v` on an absent `a`), a type
/// mismatch (`ADD`/`DELETE` against an incompatible existing type,
/// arithmetic against a non-`N` operand), an operand that evaluates to no
/// value, or a post-update item over [`MAX_ITEM_SIZE_BYTES`].
pub fn apply_update(mut item: Item, actions: &[UpdateAction]) -> Result<Item> {
    let invalid_path = || {
        UpdateError::validation(
            "The document path provided in the update expression is invalid for update",
        )
    };
    for action in actions {
        match action {
            UpdateAction::Set(path, value_expr) => {
                if !document_path_parent_exists(&item, path) {
                    return Err(invalid_path());
                }
                let value = eval_update_expr(&item, value_expr)?;
                set_document_path(&mut item, path, value);
            }
            UpdateAction::Remove(path) => {
                remove_document_path(&mut item, path);
            }
            UpdateAction::Add(path, operand) => {
                if !document_path_parent_exists(&item, path) {
                    return Err(invalid_path());
                }
                let updated = match (get_document_path(&item, path), operand) {
                    // Absent: seed with the operand. This is what makes
                    // `ADD #c :one` the idiomatic counter increment on a row
                    // that does not exist yet.
                    (None, v) => v.clone(),
                    (Some(AttributeValue::N(cur)), AttributeValue::N(delta)) => {
                        AttributeValue::N(add_numeric(cur, delta).ok_or_else(|| {
                            UpdateError::validation(format!(
                                "ADD on `{}`: `{cur}` and `{delta}` are not both numbers",
                                format_update_path(path)
                            ))
                        })?)
                    }
                    (Some(AttributeValue::SS(cur)), AttributeValue::SS(add)) => {
                        AttributeValue::SS(union_sorted(cur, add))
                    }
                    (Some(AttributeValue::NS(cur)), AttributeValue::NS(add)) => {
                        AttributeValue::NS(union_sorted(cur, add))
                    }
                    (Some(AttributeValue::BS(cur)), AttributeValue::BS(add)) => {
                        AttributeValue::BS(union_sorted(cur, add))
                    }
                    (Some(existing), operand) => {
                        return Err(UpdateError::validation(format!(
                            "ADD on `{}` needs a number or a matching set type, \
                             got {} += {}",
                            format_update_path(path),
                            type_name(existing),
                            type_name(operand)
                        )));
                    }
                };
                set_document_path(&mut item, path, updated);
            }
            UpdateAction::Delete(path, operand) => {
                let Some(existing) = get_document_path(&item, path) else {
                    // Deleting from an absent path is a no-op, as in
                    // DynamoDB — not an error.
                    continue;
                };
                let remaining = match (existing, operand) {
                    (AttributeValue::SS(cur), AttributeValue::SS(rm)) => {
                        AttributeValue::SS(difference_sorted(cur, rm))
                    }
                    (AttributeValue::NS(cur), AttributeValue::NS(rm)) => {
                        AttributeValue::NS(difference_sorted(cur, rm))
                    }
                    (AttributeValue::BS(cur), AttributeValue::BS(rm)) => {
                        AttributeValue::BS(difference_sorted(cur, rm))
                    }
                    (existing, operand) => {
                        return Err(UpdateError::validation(format!(
                            "DELETE on `{}` needs matching set types, got {} -= {}",
                            format_update_path(path),
                            type_name(existing),
                            type_name(operand)
                        )));
                    }
                };
                // DynamoDB does not store empty sets: emptying one removes the
                // attribute rather than leaving `SS: []` behind.
                if set_is_empty(&remaining) {
                    remove_document_path(&mut item, path);
                } else {
                    set_document_path(&mut item, path, remaining);
                }
            }
        }
    }
    if item_size(&item) > MAX_ITEM_SIZE_BYTES {
        return Err(UpdateError::validation(
            "Item size has exceeded the maximum allowed size",
        ));
    }
    Ok(item)
}

/// Evaluate a `SET` clause's right-hand side against `item` — the same
/// in-progress item [`apply_update`]'s own fold is building, so a `Path`
/// operand naming an attribute a prior action in the same expression
/// already set sees that action's result (see [`UpdateOperand`]'s own doc
/// on this simplification). `None` (an operand that evaluates to "no
/// value" — a bare absent path, never wrapped in `if_not_exists`) is a
/// validation error, since `SET` can never assign nothing.
fn eval_update_expr(item: &Item, expr: &UpdateExpr) -> Result<AttributeValue> {
    let missing = || {
        UpdateError::validation(
            "The provided expression refers to an attribute that does not exist in the item",
        )
    };
    match expr {
        UpdateExpr::Operand(op) => eval_update_operand(item, op)?.ok_or_else(missing),
        UpdateExpr::Add(a, b) => eval_update_arithmetic(item, a, b, "+"),
        UpdateExpr::Sub(a, b) => eval_update_arithmetic(item, a, b, "-"),
    }
}

/// The `+`/`-` arithmetic core shared by [`eval_update_expr`]'s two operator
/// arms: both operands must evaluate to `N`, added via
/// [`crate::condition::add_numeric`] (`-` first negates the right side via
/// [`crate::condition::negate_numeric`] — DynamoDB's only subtraction path).
fn eval_update_arithmetic(
    item: &Item,
    a: &UpdateOperand,
    b: &UpdateOperand,
    op: &str,
) -> Result<AttributeValue> {
    let operand_error = || {
        UpdateError::validation(format!(
            "SET arithmetic (`{op}`) requires two number (N) operands"
        ))
    };
    let av = eval_update_operand(item, a)?.ok_or_else(operand_error)?;
    let bv = eval_update_operand(item, b)?.ok_or_else(operand_error)?;
    let (AttributeValue::N(an), AttributeValue::N(bn)) = (&av, &bv) else {
        return Err(operand_error());
    };
    let rhs = if op == "-" {
        negate_numeric(bn).ok_or_else(operand_error)?
    } else {
        bn.clone()
    };
    let sum = add_numeric(an, &rhs).ok_or_else(operand_error)?;
    Ok(AttributeValue::N(sum))
}

/// Read a document path out of `item`, returning `None` at the first
/// missing key/out-of-range index or wrong-container-type step (no error —
/// the caller decides whether an absent path is legal). Returns a
/// *reference* (no clone until the caller actually needs one).
fn get_document_path<'a>(item: &'a Item, path: &[PathSegment]) -> Option<&'a AttributeValue> {
    let mut segments = path.iter();
    let PathSegment::Field(first) = segments.next()? else {
        return None; // The parser never emits an Index as a path's first segment.
    };
    let mut cur = item.get(first)?;
    for seg in segments {
        cur = match (seg, cur) {
            (PathSegment::Field(name), AttributeValue::M(m)) => m.get(name)?,
            (PathSegment::Index(idx), AttributeValue::L(l)) => l.get(*idx)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Evaluate one [`UpdateOperand`] against `item`. `Ok(None)` means the
/// operand names a document path that does not currently exist — a legal
/// intermediate result (`if_not_exists`'s first argument), but never a
/// legal *final* `SET`/arithmetic value; the caller decides whether `None`
/// is an error.
fn eval_update_operand(item: &Item, operand: &UpdateOperand) -> Result<Option<AttributeValue>> {
    match operand {
        UpdateOperand::Value(v) => Ok(Some(v.clone())),
        UpdateOperand::Path(path) => Ok(get_document_path(item, path).cloned()),
        UpdateOperand::IfNotExists(path, default) => match get_document_path(item, path) {
            Some(v) => Ok(Some(v.clone())),
            None => eval_update_operand(item, default),
        },
        UpdateOperand::ListAppend(a, b) => {
            let missing =
                || UpdateError::validation("list_append operand does not exist in the item");
            let a = eval_update_operand(item, a)?.ok_or_else(missing)?;
            let b = eval_update_operand(item, b)?.ok_or_else(missing)?;
            let (AttributeValue::L(mut av), AttributeValue::L(bv)) = (a, b) else {
                return Err(UpdateError::validation(
                    "list_append operands must both be lists (L)",
                ));
            };
            av.extend(bv);
            Ok(Some(AttributeValue::L(av)))
        }
    }
}

/// Set a document path in `item` to `value` — the target [`PathSegment`]
/// chain's parent must already exist (every segment but the last), matching
/// DynamoDB's own "The document path provided in the update expression is
/// invalid for update" rejection; only the final segment may be new. An
/// `Index` past the end of its list **appends** rather than padding
/// (DynamoDB's own documented behavior for `SET list[n]` beyond the current
/// length); an `Index` within bounds overwrites that element.
fn set_document_path(item: &mut Item, path: &[PathSegment], value: AttributeValue) {
    match path.split_first() {
        Some((PathSegment::Field(name), [])) => {
            item.insert(name.clone(), value);
        }
        Some((PathSegment::Field(name), rest)) => {
            if let Some(parent) = item.get_mut(name) {
                set_into_container(parent, rest, value);
            }
        }
        _ => {} // A top-level Index is nonsensical; the caller's own guard rejects it first.
    }
}

/// [`set_document_path`]'s recursive step once already inside a container —
/// `path` is never empty here (the caller only recurses with a non-empty
/// `rest`). A missing/wrong-shaped parent is silently a no-op: the caller
/// (`apply_update`) has already validated every intermediate segment exists
/// via [`document_path_parent_exists`] before calling this, so reaching a
/// dead end here would mean that check and this walk disagree — which
/// should never happen, not something to paper over with a second error
/// path.
fn set_into_container(container: &mut AttributeValue, path: &[PathSegment], value: AttributeValue) {
    match path.split_first() {
        None => {}
        Some((PathSegment::Field(name), rest)) => {
            let AttributeValue::M(m) = container else {
                return;
            };
            if rest.is_empty() {
                m.insert(name.clone(), value);
            } else if let Some(parent) = m.get_mut(name) {
                set_into_container(parent, rest, value);
            }
        }
        Some((PathSegment::Index(idx), rest)) => {
            let AttributeValue::L(l) = container else {
                return;
            };
            if rest.is_empty() {
                if *idx < l.len() {
                    l[*idx] = value;
                } else {
                    l.push(value);
                }
            } else if let Some(parent) = l.get_mut(*idx) {
                set_into_container(parent, rest, value);
            }
        }
    }
}

/// Whether `path`'s parent (every segment but the last) already exists in
/// `item` — the pre-flight `SET`/`ADD` guard: DynamoDB requires every
/// intermediate container to pre-exist, only the final segment may be new.
/// A bare top-level path (`path.len() == 1`) has no parent to check and is
/// always legal here.
fn document_path_parent_exists(item: &Item, path: &[PathSegment]) -> bool {
    match path.split_last() {
        None => true, // never reached: the parser never emits an empty path.
        Some((_, [])) => true,
        Some((_, parent)) => get_document_path(item, parent).is_some(),
    }
}

/// Remove a document path from `item` if present; a no-op if any part of it
/// is absent. `REMOVE a[i]` compacts the list (`Vec::remove`), matching
/// DynamoDB.
fn remove_document_path(item: &mut Item, path: &[PathSegment]) {
    match path.split_first() {
        Some((PathSegment::Field(name), [])) => {
            item.remove(name);
        }
        Some((PathSegment::Field(name), rest)) => {
            if let Some(parent) = item.get_mut(name) {
                remove_from_container(parent, rest);
            }
        }
        _ => {}
    }
}

/// [`remove_document_path`]'s recursive step once already inside a
/// container.
fn remove_from_container(container: &mut AttributeValue, path: &[PathSegment]) {
    match path.split_first() {
        None => {}
        Some((PathSegment::Field(name), rest)) => {
            let AttributeValue::M(m) = container else {
                return;
            };
            if rest.is_empty() {
                m.remove(name);
            } else if let Some(parent) = m.get_mut(name) {
                remove_from_container(parent, rest);
            }
        }
        Some((PathSegment::Index(idx), rest)) => {
            let AttributeValue::L(l) = container else {
                return;
            };
            if rest.is_empty() {
                if *idx < l.len() {
                    l.remove(*idx);
                }
            } else if let Some(parent) = l.get_mut(*idx) {
                remove_from_container(parent, rest);
            }
        }
    }
}

/// Sorted, de-duplicated union — the representation this crate keeps sets in.
fn union_sorted<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut out: Vec<T> = a.to_vec();
    out.extend(b.iter().cloned());
    out.sort();
    out.dedup();
    out
}

/// Sorted difference, `a` minus `b`.
fn difference_sorted<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().filter(|x| !b.contains(x)).cloned().collect()
}

/// Whether a set-typed value has no members.
fn set_is_empty(v: &AttributeValue) -> bool {
    match v {
        AttributeValue::SS(s) => s.is_empty(),
        AttributeValue::NS(s) => s.is_empty(),
        AttributeValue::BS(s) => s.is_empty(),
        _ => false,
    }
}

/// A human-readable type name for an error message.
fn type_name(v: &AttributeValue) -> &'static str {
    match v {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::M(_) => "M",
        AttributeValue::L(_) => "L",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    /// A one-segment field path — the common case in these tests.
    fn field(name: &str) -> Vec<PathSegment> {
        vec![PathSegment::Field(name.into())]
    }

    #[test]
    fn apply_update_sets_and_removes() {
        let mut item = Item::new();
        item.insert("id".into(), s("k"));
        item.insert("c".into(), s("drop"));
        let new = apply_update(
            item,
            &[
                UpdateAction::Set(field("a"), UpdateExpr::value(s("x"))),
                UpdateAction::Remove(field("c")),
            ],
        )
        .expect("SET/REMOVE are infallible");
        assert_eq!(new.get("a"), Some(&s("x")));
        assert!(!new.contains_key("c"));
        assert_eq!(new.get("id"), Some(&s("k")));
    }

    #[test]
    fn if_not_exists_seeds_an_absent_attribute_and_leaves_a_present_one_alone() {
        let action = UpdateAction::Set(
            field("a"),
            UpdateExpr::Operand(UpdateOperand::IfNotExists(
                field("a"),
                Box::new(UpdateOperand::Value(AttributeValue::N("0".into()))),
            )),
        );
        let out =
            apply_update(Item::new(), std::slice::from_ref(&action)).expect("seeds the default");
        assert_eq!(out.get("a"), Some(&AttributeValue::N("0".into())));

        let mut present = Item::new();
        present.insert("a".into(), AttributeValue::N("7".into()));
        let out = apply_update(present, &[action]).expect("leaves the existing value alone");
        assert_eq!(out.get("a"), Some(&AttributeValue::N("7".into())));
    }

    /// `if_not_exists(a, :v)` where `a` is absent and `:v` is itself an
    /// absent-path default (never wrapped in another `if_not_exists`) has no
    /// value to assign — a validation error, not a silently-applied no-op.
    #[test]
    fn if_not_exists_with_no_default_value_is_a_validation_error() {
        let action = UpdateAction::Set(
            field("a"),
            UpdateExpr::Operand(UpdateOperand::IfNotExists(
                field("a"),
                Box::new(UpdateOperand::Path(field("also_absent"))),
            )),
        );
        let err = apply_update(Item::new(), &[action]).expect_err("nothing to assign");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn list_append_concatenates_in_order() {
        let mut item = Item::new();
        item.insert(
            "a".into(),
            AttributeValue::L(vec![AttributeValue::N("1".into())]),
        );
        let action = UpdateAction::Set(
            field("a"),
            UpdateExpr::Operand(UpdateOperand::ListAppend(
                Box::new(UpdateOperand::Path(field("a"))),
                Box::new(UpdateOperand::Value(AttributeValue::L(vec![
                    AttributeValue::N("2".into()),
                ]))),
            )),
        );
        let out = apply_update(item, &[action]).expect("both operands are lists");
        assert_eq!(
            out.get("a"),
            Some(&AttributeValue::L(vec![
                AttributeValue::N("1".into()),
                AttributeValue::N("2".into()),
            ]))
        );
    }

    #[test]
    fn list_append_on_a_non_list_operand_is_a_validation_error() {
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::N("1".into()));
        let action = UpdateAction::Set(
            field("a"),
            UpdateExpr::Operand(UpdateOperand::ListAppend(
                Box::new(UpdateOperand::Path(field("a"))),
                Box::new(UpdateOperand::Value(AttributeValue::L(vec![]))),
            )),
        );
        let err = apply_update(item, &[action]).expect_err("a is a number, not a list");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn list_append_on_a_missing_operand_is_a_validation_error() {
        let action = UpdateAction::Set(
            field("a"),
            UpdateExpr::Operand(UpdateOperand::ListAppend(
                Box::new(UpdateOperand::Path(field("missing"))),
                Box::new(UpdateOperand::Value(AttributeValue::L(vec![]))),
            )),
        );
        let err = apply_update(Item::new(), &[action]).expect_err("`missing` does not exist");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn set_arithmetic_adds_and_subtracts_with_decimal_precision() {
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::N("1.10".into()));
        let add = UpdateAction::Set(
            field("a"),
            UpdateExpr::Add(
                UpdateOperand::Path(field("a")),
                UpdateOperand::Value(AttributeValue::N("0.90".into())),
            ),
        );
        let out = apply_update(item, &[add]).expect("both sides numeric");
        assert_eq!(out.get("a"), Some(&AttributeValue::N("2".into())));

        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::N("5".into()));
        let sub = UpdateAction::Set(
            field("a"),
            UpdateExpr::Sub(
                UpdateOperand::Path(field("a")),
                UpdateOperand::Value(AttributeValue::N("8".into())),
            ),
        );
        let out = apply_update(item, &[sub]).expect("both sides numeric");
        assert_eq!(out.get("a"), Some(&AttributeValue::N("-3".into())));
    }

    /// A non-`N` operand on either side of `+`/`-` is a validation error.
    #[test]
    fn set_arithmetic_rejects_a_non_numeric_operand() {
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::S("nope".into()));
        let add = UpdateAction::Set(
            field("a"),
            UpdateExpr::Add(
                UpdateOperand::Path(field("a")),
                UpdateOperand::Value(AttributeValue::N("1".into())),
            ),
        );
        let err = apply_update(item, &[add]).expect_err("a is an S, not N");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn apply_update_sets_a_nested_field_when_the_parent_exists() {
        let mut nested = std::collections::BTreeMap::new();
        nested.insert("b".into(), AttributeValue::N("1".into()));
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::M(nested));
        let action = UpdateAction::Set(
            vec![
                PathSegment::Field("a".into()),
                PathSegment::Field("c".into()),
            ],
            UpdateExpr::value(s("new")),
        );
        let out = apply_update(item, &[action]).expect("a already exists");
        let AttributeValue::M(m) = out.get("a").expect("a present") else {
            panic!("a is not a map");
        };
        assert_eq!(
            m.get("b"),
            Some(&AttributeValue::N("1".into())),
            "sibling untouched"
        );
        assert_eq!(m.get("c"), Some(&s("new")), "new key added");
    }

    /// `SET a.b = :v` on a completely absent `a` is a validation error —
    /// only the *final* path segment may be new.
    #[test]
    fn apply_update_rejects_set_on_a_nested_path_whose_parent_is_missing() {
        let action = UpdateAction::Set(
            vec![
                PathSegment::Field("a".into()),
                PathSegment::Field("b".into()),
            ],
            UpdateExpr::value(s("x")),
        );
        let err = apply_update(Item::new(), &[action]).expect_err("a does not exist");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn apply_update_sets_a_list_index_in_bounds_and_appends_out_of_bounds() {
        let mut item = Item::new();
        item.insert(
            "l".into(),
            AttributeValue::L(vec![
                AttributeValue::N("0".into()),
                AttributeValue::N("1".into()),
            ]),
        );
        // In-bounds: overwrite index 0.
        let out = apply_update(
            item,
            &[UpdateAction::Set(
                vec![PathSegment::Field("l".into()), PathSegment::Index(0)],
                UpdateExpr::value(AttributeValue::N("99".into())),
            )],
        )
        .expect("index 0 is in bounds");
        assert_eq!(
            out.get("l"),
            Some(&AttributeValue::L(vec![
                AttributeValue::N("99".into()),
                AttributeValue::N("1".into()),
            ]))
        );

        // Out-of-bounds: appends rather than padding or erroring (AWS's own
        // documented `SET list[n]` behavior beyond the current length).
        let out = apply_update(
            out,
            &[UpdateAction::Set(
                vec![PathSegment::Field("l".into()), PathSegment::Index(10)],
                UpdateExpr::value(AttributeValue::N("2".into())),
            )],
        )
        .expect("out-of-bounds SET appends");
        assert_eq!(
            out.get("l"),
            Some(&AttributeValue::L(vec![
                AttributeValue::N("99".into()),
                AttributeValue::N("1".into()),
                AttributeValue::N("2".into()),
            ]))
        );
    }

    #[test]
    fn apply_update_removes_a_nested_field_and_compacts_a_list_index() {
        let mut nested = std::collections::BTreeMap::new();
        nested.insert("b".into(), AttributeValue::N("1".into()));
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::M(nested));
        item.insert(
            "l".into(),
            AttributeValue::L(vec![
                AttributeValue::N("0".into()),
                AttributeValue::N("1".into()),
                AttributeValue::N("2".into()),
            ]),
        );
        let out = apply_update(
            item,
            &[
                UpdateAction::Remove(vec![
                    PathSegment::Field("a".into()),
                    PathSegment::Field("b".into()),
                ]),
                UpdateAction::Remove(vec![PathSegment::Field("l".into()), PathSegment::Index(1)]),
            ],
        )
        .expect("REMOVE is infallible");
        let AttributeValue::M(m) = out.get("a").expect("a present") else {
            panic!("a is not a map");
        };
        assert!(!m.contains_key("b"), "b removed");
        // Removing index 1 compacts the list — no hole left behind.
        assert_eq!(
            out.get("l"),
            Some(&AttributeValue::L(vec![
                AttributeValue::N("0".into()),
                AttributeValue::N("2".into()),
            ]))
        );
    }

    /// `REMOVE` on a path that does not exist (missing key, missing parent,
    /// or an out-of-range index) is a no-op, not an error.
    #[test]
    fn apply_update_removes_a_missing_nested_path_as_a_no_op() {
        let out = apply_update(
            Item::new(),
            &[UpdateAction::Remove(vec![
                PathSegment::Field("a".into()),
                PathSegment::Field("b".into()),
            ])],
        )
        .expect("no-op, not an error");
        assert!(out.is_empty());
    }

    /// `ADD`/`DELETE` also target nested paths: the same get/set-document-path
    /// primitives `SET`/`REMOVE` use.
    #[test]
    fn add_and_delete_target_a_nested_path() {
        let mut nested = std::collections::BTreeMap::new();
        nested.insert("count".into(), AttributeValue::N("41".into()));
        let mut item = Item::new();
        item.insert("stats".into(), AttributeValue::M(nested));
        let out = apply_update(
            item,
            &[UpdateAction::Add(
                vec![
                    PathSegment::Field("stats".into()),
                    PathSegment::Field("count".into()),
                ],
                AttributeValue::N("1".into()),
            )],
        )
        .expect("ADD on a nested N");
        let AttributeValue::M(m) = out.get("stats").expect("stats present") else {
            panic!("stats is not a map");
        };
        assert_eq!(m.get("count"), Some(&AttributeValue::N("42".into())));
    }

    /// `apply_update`'s post-fold result is checked against the same cap:
    /// exactly `MAX_ITEM_SIZE_BYTES` is accepted, one byte over is rejected.
    /// This is the choke point both `UpdateItem` and `TransactWriteItems`'s
    /// `Update` action route through, so covering it here covers both.
    #[test]
    fn apply_update_result_accepts_exactly_the_size_cap_and_rejects_one_byte_over() {
        // "a" is a 1-byte attribute name, so a value of `MAX_ITEM_SIZE_BYTES -
        // 1` bytes makes the post-update item land exactly on the cap.
        let at_cap_value = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES - 1));
        let out = apply_update(
            Item::new(),
            &[UpdateAction::Set(
                field("a"),
                UpdateExpr::value(at_cap_value),
            )],
        )
        .expect("exactly the cap is accepted");
        assert_eq!(item_size(&out), MAX_ITEM_SIZE_BYTES);

        let over_cap_value = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES));
        let err = apply_update(
            Item::new(),
            &[UpdateAction::Set(
                field("a"),
                UpdateExpr::value(over_cap_value),
            )],
        )
        .expect_err("one byte over the cap is rejected");
        assert_eq!(err.code, "ValidationException");
        assert!(
            err.message
                .contains("Item size has exceeded the maximum allowed size")
        );
    }

    /// An item temporarily over the cap **mid-fold** must still succeed if
    /// the fold's own later action nets the *final* result back under it —
    /// the check runs once, after the whole action list folds, never
    /// mid-fold. Ordered `SET` (pushes it over) then `REMOVE` (nets it back
    /// under) so the over-size state genuinely occurs before the netting.
    #[test]
    fn apply_update_nets_under_the_cap_after_an_over_size_mid_fold_state() {
        let mut item = Item::new();
        item.insert("keep".into(), s("k"));

        let huge = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES));
        let out = apply_update(
            item,
            &[
                UpdateAction::Set(field("temp"), UpdateExpr::value(huge)),
                UpdateAction::Remove(field("temp")),
            ],
        )
        .expect("nets back under the cap after the REMOVE, so it must succeed");
        assert!(!out.contains_key("temp"));
        assert_eq!(out.get("keep"), Some(&s("k")));
    }

    /// An update to an already-near-cap base item whose result tips over the
    /// cap is rejected — the pre-update image being under the cap does not
    /// exempt the post-update one.
    #[test]
    fn apply_update_rejects_when_it_tips_a_near_cap_base_item_over() {
        let mut item = Item::new();
        item.insert(
            "a".into(),
            AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES - 10)),
        );
        // base item size = 1 ("a") + (MAX_ITEM_SIZE_BYTES - 10) = MAX_ITEM_SIZE_BYTES - 9.

        let err = apply_update(
            item,
            &[UpdateAction::Set(
                field("b"),
                UpdateExpr::value(AttributeValue::S("y".repeat(20))),
            )],
        )
        // adds 1 ("b") + 20 = 21 bytes -> MAX_ITEM_SIZE_BYTES + 12, over the cap.
        .expect_err("tips the near-cap base item over the cap");
        assert_eq!(err.code, "ValidationException");
    }

    /// `ADD` seeds an absent attribute, increments a number, and unions a set.
    #[test]
    fn add_seeds_increments_and_unions() {
        let n = |v: &str| AttributeValue::N(v.into());
        let ss = |v: &[&str]| AttributeValue::SS(v.iter().map(|s| (*s).to_string()).collect());

        // Absent -> seeded. This is the counter-on-a-new-row case.
        let out =
            apply_update(Item::new(), &[UpdateAction::Add(field("c"), n("1"))]).expect("applies");
        assert_eq!(out.get("c"), Some(&n("1")));

        // Present -> incremented, exactly.
        let mut item = Item::new();
        item.insert("c".into(), n("41"));
        let out = apply_update(item, &[UpdateAction::Add(field("c"), n("1"))]).expect("applies");
        assert_eq!(out.get("c"), Some(&n("42")));

        // Sets union and stay sorted/deduplicated.
        let mut item = Item::new();
        item.insert("t".into(), ss(&["a", "b"]));
        let out =
            apply_update(item, &[UpdateAction::Add(field("t"), ss(&["b", "c"]))]).expect("applies");
        assert_eq!(
            out.get("t"),
            Some(&ss(&["a", "b", "c"])),
            "union, deduplicated"
        );
    }

    /// `DELETE` subtracts set members, and emptying a set removes the
    /// attribute — DynamoDB does not store empty sets.
    #[test]
    fn delete_subtracts_and_drops_an_emptied_set() {
        let ss = |v: &[&str]| AttributeValue::SS(v.iter().map(|s| (*s).to_string()).collect());

        let mut item = Item::new();
        item.insert("t".into(), ss(&["a", "b", "c"]));
        let out =
            apply_update(item, &[UpdateAction::Delete(field("t"), ss(&["b"]))]).expect("applies");
        assert_eq!(out.get("t"), Some(&ss(&["a", "c"])));

        let mut item = Item::new();
        item.insert("t".into(), ss(&["a"]));
        let out =
            apply_update(item, &[UpdateAction::Delete(field("t"), ss(&["a"]))]).expect("applies");
        assert!(
            !out.contains_key("t"),
            "an emptied set is removed, not stored as an empty set: {out:?}"
        );

        // Deleting from an absent attribute is a no-op, not an error.
        let out = apply_update(Item::new(), &[UpdateAction::Delete(field("t"), ss(&["a"]))])
            .expect("no-op");
        assert!(out.is_empty());
    }

    /// A typed mismatch is an error, never a silently skipped action — the
    /// caller must not believe an update applied when it did not.
    #[test]
    fn add_and_delete_reject_type_mismatches() {
        let mut item = Item::new();
        item.insert("s".into(), AttributeValue::S("text".into()));
        assert!(
            apply_update(
                item.clone(),
                &[UpdateAction::Add(field("s"), AttributeValue::N("1".into()))]
            )
            .is_err(),
            "ADD a number to a string must be rejected"
        );
        assert!(
            apply_update(
                item,
                &[UpdateAction::Delete(
                    field("s"),
                    AttributeValue::SS(vec!["a".into()])
                )]
            )
            .is_err(),
            "DELETE a set from a string must be rejected"
        );
    }
}
