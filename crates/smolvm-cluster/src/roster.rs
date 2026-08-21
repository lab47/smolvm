//! The frontend's live view of backends, built from gossip announcements.
//!
//! A cache, not a source of truth: entries arrive via `Announce`, refresh their
//! `last_seen` on each one, and are evicted either on `Withdraw` or after a TTL
//! (a few missed announce intervals). Placement (Phase 3) and by-name routing
//! (Phase 4) read this; Phase 2 forwards to whichever backend is present.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use iroh::{EndpointAddr, EndpointId};

/// How often backends announce themselves.
pub const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5);
/// Drop a backend we haven't heard from in this long (~3 missed announces).
pub const ROSTER_TTL: Duration = Duration::from_secs(16);

#[derive(Debug, Clone)]
struct Entry {
    addr: EndpointAddr,
    epoch: u64,
    last_seen: Instant,
}

/// A backend known to the frontend.
#[derive(Debug, Clone)]
pub struct Backend {
    pub id: EndpointId,
    pub addr: EndpointAddr,
    pub epoch: u64,
    pub age: Duration,
}

/// Thread-safe roster of live backends.
#[derive(Default)]
pub struct Roster {
    entries: Mutex<HashMap<EndpointId, Entry>>,
}

impl Roster {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record (or refresh) a backend from an `Announce`. A stale announce — one
    /// with an epoch older than what we already have — is ignored.
    pub fn upsert(&self, addr: EndpointAddr, epoch: u64) {
        let id = addr.id;
        let mut map = self.entries.lock().unwrap();
        match map.get_mut(&id) {
            Some(existing) if epoch < existing.epoch => {} // stale restart echo
            Some(existing) => {
                existing.addr = addr;
                existing.epoch = epoch;
                existing.last_seen = Instant::now();
            }
            None => {
                map.insert(
                    id,
                    Entry {
                        addr,
                        epoch,
                        last_seen: Instant::now(),
                    },
                );
            }
        }
    }

    /// Remove a backend (graceful `Withdraw`). Returns whether it was present.
    pub fn remove(&self, id: &EndpointId) -> bool {
        self.entries.lock().unwrap().remove(id).is_some()
    }

    /// Evict entries not seen within the TTL. Returns the ids evicted.
    pub fn sweep(&self) -> Vec<EndpointId> {
        let mut map = self.entries.lock().unwrap();
        let now = Instant::now();
        let dead: Vec<EndpointId> = map
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_seen) > ROSTER_TTL)
            .map(|(id, _)| *id)
            .collect();
        for id in &dead {
            map.remove(id);
        }
        dead
    }

    /// Snapshot of the current live backends.
    pub fn snapshot(&self) -> Vec<Backend> {
        let map = self.entries.lock().unwrap();
        let now = Instant::now();
        map.values()
            .map(|e| Backend {
                id: e.addr.id,
                addr: e.addr.clone(),
                epoch: e.epoch,
                age: now.duration_since(e.last_seen),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn addr() -> EndpointAddr {
        EndpointAddr::new(SecretKey::generate().public())
    }

    #[test]
    fn upsert_remove_and_stale_epoch() {
        let r = Roster::new();
        let a = addr();
        let id = a.id;
        r.upsert(a.clone(), 5);
        assert_eq!(r.len(), 1);
        // stale epoch ignored (still present, epoch unchanged)
        r.upsert(a.clone(), 3);
        assert_eq!(r.snapshot()[0].epoch, 5);
        // newer epoch wins
        r.upsert(a.clone(), 9);
        assert_eq!(r.snapshot()[0].epoch, 9);
        assert!(r.remove(&id));
        assert!(r.is_empty());
    }
}
