//! The cluster's dynamic membership view.
//!
//! Every node keeps a set of members `{id, role}`. It propagates over the cluster
//! connections (each side periodically announces what it can vouch for), and the
//! dialer keeps a connection to every frontend the view names. New members ripple
//! outward reliably — every hop is a plain reliable stream, no gossip library.
//!
//! Liveness lives in the connections, not here: a node announces only itself plus
//! members it's *directly connected to*, so a departed member stops being
//! announced once its links drop and then ages out of everyone's view.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use iroh::EndpointId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MemberRole {
    Frontend,
    Backend,
}

struct Entry {
    role: MemberRole,
    last_heard: Instant,
    connected: bool,
}

pub struct Membership {
    self_id: EndpointId,
    self_role: MemberRole,
    ttl: Duration,
    inner: Mutex<HashMap<EndpointId, Entry>>,
}

impl Membership {
    pub fn new(self_id: EndpointId, self_role: MemberRole) -> Self {
        Self {
            self_id,
            self_role,
            ttl: Duration::from_secs(60),
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Seed the view with bootstrap frontend ids (the `--cluster-bootstrap`s).
    pub fn seed_frontends(&self, ids: impl IntoIterator<Item = EndpointId>) {
        let mut m = self.inner.lock().unwrap();
        for id in ids {
            if id != self.self_id {
                m.entry(id).or_insert(Entry {
                    role: MemberRole::Frontend,
                    last_heard: Instant::now(),
                    connected: false,
                });
            }
        }
    }

    /// Merge a peer's announced view; returns frontend ids we hadn't seen before
    /// (so the dialer can start connecting to them).
    pub fn merge(&self, entries: &[(EndpointId, MemberRole)]) -> Vec<EndpointId> {
        let mut m = self.inner.lock().unwrap();
        let mut fresh = Vec::new();
        for (id, role) in entries {
            if *id == self.self_id {
                continue;
            }
            match m.get_mut(id) {
                Some(e) => {
                    e.role = *role;
                    e.last_heard = Instant::now();
                }
                None => {
                    m.insert(
                        *id,
                        Entry {
                            role: *role,
                            last_heard: Instant::now(),
                            connected: false,
                        },
                    );
                    if *role == MemberRole::Frontend {
                        fresh.push(*id);
                    }
                }
            }
        }
        fresh
    }

    pub fn connected(&self, id: EndpointId, role: MemberRole) {
        let mut m = self.inner.lock().unwrap();
        let e = m.entry(id).or_insert(Entry {
            role,
            last_heard: Instant::now(),
            connected: true,
        });
        e.role = role;
        e.connected = true;
        e.last_heard = Instant::now();
    }

    pub fn is_connected(&self, id: EndpointId) -> bool {
        self.inner
            .lock()
            .unwrap()
            .get(&id)
            .map(|e| e.connected)
            .unwrap_or(false)
    }

    pub fn disconnected(&self, id: EndpointId) {
        if let Some(e) = self.inner.lock().unwrap().get_mut(&id) {
            e.connected = false;
            e.last_heard = Instant::now();
        }
    }

    /// What to announce: self + members we're directly connected to.
    pub fn announced_view(&self) -> Vec<(EndpointId, MemberRole)> {
        let m = self.inner.lock().unwrap();
        let mut v = vec![(self.self_id, self.self_role)];
        for (id, e) in m.iter() {
            if e.connected {
                v.push((*id, e.role));
            }
        }
        v
    }

    /// Known frontends (except self) — the set the dialer maintains links to.
    pub fn frontends(&self) -> Vec<EndpointId> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, e)| e.role == MemberRole::Frontend && **id != self.self_id)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Evict unconnected members not heard from within the TTL.
    pub fn sweep(&self) {
        let now = Instant::now();
        let ttl = self.ttl;
        self.inner
            .lock()
            .unwrap()
            .retain(|_, e| e.connected || now.duration_since(e.last_heard) < ttl);
    }

    /// Introspection: `(id, role, connected, age_ms)`, self included.
    pub fn snapshot(&self) -> Vec<(EndpointId, MemberRole, bool, u64)> {
        let now = Instant::now();
        let mut v: Vec<_> = self
            .inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, e)| {
                (
                    *id,
                    e.role,
                    e.connected,
                    now.duration_since(e.last_heard).as_millis() as u64,
                )
            })
            .collect();
        v.push((self.self_id, self.self_role, true, 0));
        v
    }
}
