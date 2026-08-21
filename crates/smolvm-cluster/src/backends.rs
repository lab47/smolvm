//! The frontend's table of connected backends: the connection to forward over
//! plus the latest capacity each pushed. A backend is present exactly while its
//! connection is open; iroh upgrades that connection to a direct path on its own
//! when one is reachable.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use iroh::endpoint::Connection;
use iroh::EndpointId;

use crate::capacity::{score, BidSpec, CapacitySnapshot};

struct Backend {
    conn: Connection,
    capacity: Option<CapacitySnapshot>,
    last_capacity: Instant,
}

#[derive(Default)]
pub struct BackendTable {
    inner: Mutex<HashMap<EndpointId, Backend>>,
}

impl BackendTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, id: EndpointId, conn: Connection) {
        self.inner.lock().unwrap().insert(
            id,
            Backend {
                conn,
                capacity: None,
                last_capacity: Instant::now(),
            },
        );
    }

    pub fn update_capacity(&self, id: EndpointId, cap: CapacitySnapshot) {
        if let Some(b) = self.inner.lock().unwrap().get_mut(&id) {
            b.capacity = Some(cap);
            b.last_capacity = Instant::now();
        }
    }

    pub fn remove(&self, id: &EndpointId) -> bool {
        self.inner.lock().unwrap().remove(id).is_some()
    }

    pub fn forward_conn(&self, id: &EndpointId) -> Option<Connection> {
        self.inner.lock().unwrap().get(id).map(|b| b.conn.clone())
    }

    /// Best backends for `spec` by capacity score: `(id, connection, score)`,
    /// best first. Skips backends with no capacity yet or that can't fit.
    pub fn best_for(&self, spec: BidSpec) -> Vec<(EndpointId, Connection, f64)> {
        let map = self.inner.lock().unwrap();
        let mut scored: Vec<(EndpointId, Connection, f64)> = map
            .iter()
            .filter_map(|(id, b)| {
                let cap = b.capacity.as_ref()?;
                let s = score(cap, &spec)?;
                Some((*id, b.conn.clone(), s))
            })
            .collect();
        scored.sort_by(|a, c| c.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        scored
    }

    pub fn all(&self) -> Vec<(EndpointId, Connection)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, b)| (*id, b.conn.clone()))
            .collect()
    }

    /// Introspection: `(id, forward_path_is_direct, capacity, age_ms)`.
    pub fn snapshot(&self) -> Vec<(EndpointId, bool, Option<CapacitySnapshot>, u64)> {
        let now = Instant::now();
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, b)| {
                let direct = b.conn.paths().iter().any(|p| p.is_ip());
                (
                    *id,
                    direct,
                    b.capacity,
                    now.duration_since(b.last_capacity).as_millis() as u64,
                )
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}
