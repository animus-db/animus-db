//! The `#[wasm_bindgen]` face of the browser build: the JS class
//! `AnimusCluster`. All logic lives in [`crate::core`]; this file only
//! translates between JS values and it.

use std::collections::BTreeMap;

use js_sys::{Function, Object, Promise, Reflect};
use wasm_bindgen::prelude::*;

use crate::core::{Completed, Core};

#[wasm_bindgen]
extern "C" {
    /// `console.error`, for the panic hook.
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error(message: &str);
}

/// Route a Rust panic message to `console.error` (the default hook writes to a
/// stderr that a browser does not have, leaving only an opaque "unreachable").
fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| console_error(&info.to_string())));
}

/// A whole AnimusDB cluster in this tab, on a virtual clock the page drives.
#[wasm_bindgen]
pub struct AnimusCluster {
    core: Core,
    /// The `resolve` function of each request's promise.
    resolvers: BTreeMap<u64, Function>,
}

fn err(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

/// A JS safe-integer-valued number to `u64`, or an error naming `what`.
fn index(value: f64, what: &str) -> Result<u64, JsError> {
    if value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= 9_007_199_254_740_991.0
    {
        // Checked above: a non-negative integer below 2^53 converts exactly.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Ok(value as u64)
    } else {
        Err(JsError::new(&format!(
            "{what} must be a non-negative integer, got {value}"
        )))
    }
}

#[wasm_bindgen]
impl AnimusCluster {
    /// Build a `nodes`-node cluster from `seed` (a non-negative integer) and let
    /// its control group elect a leader. Takes a moment: it runs several
    /// seconds of virtual time.
    ///
    /// # Errors
    /// A bad `seed` or a `nodes` outside `1..=9`.
    #[wasm_bindgen(constructor)]
    pub fn new(seed: f64, nodes: u32) -> Result<AnimusCluster, JsError> {
        install_panic_hook();
        let core = Core::new(index(seed, "seed")?, nodes as usize).map_err(err)?;
        Ok(Self {
            core,
            resolvers: BTreeMap::new(),
        })
    }

    /// Send a DynamoDB-wire request (`target` is the `X-Amz-Target` value, for
    /// example `DynamoDB_20120810.PutItem`; `body` the JSON request) to `node`.
    /// The promise resolves to `{ status, body }` (`body` is the response JSON
    /// string) once `tick` has run the cluster far enough; it rejects for a
    /// node that does not exist. A request unanswered after 30 virtual seconds
    /// resolves with status 504.
    #[wasm_bindgen]
    pub fn request(&mut self, node: u32, target: &str, body: &str) -> Promise {
        let mut fns: Option<(Function, Function)> = None;
        let promise = Promise::new(&mut |resolve, reject| fns = Some((resolve, reject)));
        let Some((resolve, reject)) = fns else {
            return promise;
        };
        match self.core.submit(u64::from(node), target, body) {
            Ok(id) => {
                self.resolvers.insert(id, resolve);
            }
            Err(e) => {
                let _ = reject.call1(&JsValue::NULL, &JsValue::from_str(&e.to_string()));
            }
        }
        promise
    }

    /// Advance virtual time by `real_ms * speed` (call once per
    /// `requestAnimationFrame` with the elapsed milliseconds; `real_ms` counts
    /// at most 100 and `speed` at most 100) and settle the promises of requests
    /// that finished. Returns how many requests were settled.
    #[wasm_bindgen]
    pub fn tick(&mut self, real_ms: f64, speed: f64) -> u32 {
        let done = self.core.tick(real_ms, speed);
        let count = done.len();
        for c in done {
            self.settle(c);
        }
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    fn settle(&mut self, c: Completed) {
        let Some(resolve) = self.resolvers.remove(&c.id) else {
            return;
        };
        let result = Object::new();
        let _ = Reflect::set(
            &result,
            &"status".into(),
            &JsValue::from_f64(f64::from(c.status)),
        );
        let _ = Reflect::set(&result, &"body".into(), &JsValue::from_str(&c.body));
        let _ = resolve.call1(&JsValue::NULL, &result);
    }

    /// Crash `node`: it stops serving and keeps its durable state.
    ///
    /// # Errors
    /// `node` does not exist.
    #[wasm_bindgen]
    pub fn crash(&mut self, node: u32) -> Result<(), JsError> {
        self.core.crash(u64::from(node)).map_err(err)
    }

    /// Restart `node` as a fresh process on its surviving state.
    ///
    /// # Errors
    /// `node` does not exist.
    #[wasm_bindgen]
    pub fn restart(&mut self, node: u32) -> Result<(), JsError> {
        self.core.restart(u64::from(node)).map_err(err)?;
        // Requests that were in flight on it are answered 503 at once.
        self.tick(0.0, 1.0);
        Ok(())
    }

    /// Cut `a` and `b` off from each other until `healAll`.
    ///
    /// # Errors
    /// A node does not exist, or `a == b`.
    #[wasm_bindgen]
    pub fn partition(&mut self, a: u32, b: u32) -> Result<(), JsError> {
        self.core.partition(u64::from(a), u64::from(b)).map_err(err)
    }

    /// Heal every partition and un-mute every crashed node.
    #[wasm_bindgen(js_name = healAll)]
    pub fn heal_all(&mut self) {
        self.core.heal_all();
    }

    /// Add a node, wait (in virtual time) for the cluster to admit it, and
    /// return its index.
    ///
    /// # Errors
    /// The cluster is already at 9 nodes, or a node is down or a partition is in
    /// place (heal first).
    #[wasm_bindgen]
    pub fn grow(&mut self) -> Result<u32, JsError> {
        let node = self.core.grow().map_err(err)?;
        u32::try_from(node).map_err(err)
    }

    /// The cluster snapshot as a JSON string: `seed`, `virtual_time_ms`,
    /// `control_leader`, `nodes[{id, up, control_leader, hosted_tablets,
    /// led_tablets}]`, `tables`, `tablets`, `partitions`, `pending_requests`,
    /// `task_polls`, `timer_fires`.
    #[wasm_bindgen]
    pub fn status(&self) -> String {
        self.core.status_json()
    }
}
