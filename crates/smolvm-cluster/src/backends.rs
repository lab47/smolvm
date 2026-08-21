//! The frontend's registry of connected backends.
//!
//! A backend is "in the cluster" exactly while its connection to the frontend is
//! open — that connection *is* discovery and liveness, no gossip. Each backend may
//! hold two connections: the `inbound` one it dialed (always present; carries the
//! capacity pushes) and, when the inbound path is only a relay, a `direct` one the
//! frontend dialed back to get a low-latency path. Forwards prefer the direct one.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use iroh::endpoint::Connection;
use iroh::EndpointId;

use crate::capacity::{score, BidSpec, CapacitySnapshot};

struct Backend {
    inbound: Connection,
    direct: Option<Connection>,
    capacity: Option<CapacitySnapshot>,
    last_capacity: Instant,
}

impl Backend {
    /// The connection to forward over: the direct one if we have it, else inbound.
    fn forward(&self) -> Connection {
        self.direct.clone().unwrap_or_else(|| self.inbound.clone())
    }
}

#[derive(Default)]
pub struct BackendTable {
    inner: Mutex<HashMap<EndpointId, Backend>>,
}

impl BackendTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a backend's inbound connection.
    pub fn register(&self, id: EndpointId, inbound: Connection) {
        self.inner.lock().unwrap().insert(
            id,
            Backend {
                inbound,
                direct: None,
                capacity: None,
                last_capacity: Instant::now(),
            },
        );
    }

    /// Record the frontend's dial-back direct connection to a backend.
    pub fn set_direct(&self, id: EndpointId, direct: Connection) {
        if let Some(b) = self.inner.lock().unwrap().get_mut(&id) {
            b.direct = Some(direct);
        }
    }

    /// Update a backend's pushed capacity (also refreshes liveness).
    pub fn update_capacity(&self, id: EndpointId, cap: CapacitySnapshot) {
        if let Some(b) = self.inner.lock().unwrap().get_mut(&id) {
            b.capacity = Some(cap);
            b.last_capacity = Instant::now();
        }
    }

    /// Remove a backend (its connection closed). True if it was present.
    pub fn remove(&self, id: &EndpointId) -> bool {
        self.inner.lock().unwrap().remove(id).is_some()
    }

    /// The forward connection for a specific backend, if present.
    pub fn forward_conn(&self, id: &EndpointId) -> Option<Connection> {
        self.inner.lock().unwrap().get(id).map(Backend::forward)
    }

    /// Best backend for `spec` by capacity score: `(id, forward connection)`.
    /// Backends that haven't pushed capacity yet, or can't fit, are skipped.
    pub fn best_for(&self, spec: BidSpec) -> Vec<(EndpointId, Connection, f64)> {
        let map = self.inner.lock().unwrap();
        let mut scored: Vec<(EndpointId, Connection, f64)> = map
            .iter()
            .filter_map(|(id, b)| {
                let cap = b.capacity.as_ref()?;
                let s = score(cap, &spec)?;
                Some((*id, b.forward(), s))
            })
            .collect();
        scored.sort_by(|a, c| c.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        scored
    }

    /// Every backend's forward connection (for fan-out list/probe).
    pub fn all(&self) -> Vec<(EndpointId, Connection)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, b)| (*id, b.forward()))
            .collect()
    }

    /// Introspection snapshot: `(id, forward_path_is_direct, capacity, age_ms)`.
    /// `forward_path_is_direct` reflects the *actual* path of the connection we'd
    /// forward over (an IP path is direct; otherwise it's the relay), whether that
    /// came from the inbound connection upgrading itself or from a dial-back.
    pub fn snapshot(&self) -> Vec<(EndpointId, bool, Option<CapacitySnapshot>, u64)> {
        let now = Instant::now();
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, b)| {
                let direct = b.forward().paths().iter().any(|p| p.is_ip());
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
