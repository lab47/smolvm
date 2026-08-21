//! The frontend's authoritative record of the sandboxes it has placed.
//!
//! A sandbox is the unit a client asks for; placing one is what decides which
//! backend runs it. The sandbox's id doubles as the machine name on that backend
//! (they're the same value), so this map — sandbox id → owning backend — is all
//! the frontend needs to route every follow-up request to the right place.
//!
//! It is authoritative but not the *only* source of truth: a lookup miss (e.g.
//! after a frontend restart) is recovered by probing the backends, so a lost map
//! self-heals rather than misrouting.

use std::collections::HashMap;
use std::sync::Mutex;

use iroh::EndpointId;

/// Where a placed sandbox lives.
#[derive(Default)]
pub struct SandboxRegistry {
    placements: Mutex<HashMap<String, EndpointId>>,
}

impl SandboxRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `sandbox` was placed on `backend` (called at allocation, and
    /// to repopulate after a recovery probe).
    pub fn place(&self, sandbox: &str, backend: EndpointId) {
        self.placements
            .lock()
            .unwrap()
            .insert(sandbox.to_string(), backend);
    }

    /// The backend owning `sandbox`, if known.
    pub fn locate(&self, sandbox: &str) -> Option<EndpointId> {
        self.placements.lock().unwrap().get(sandbox).copied()
    }

    /// Whether we already track `sandbox` (used for the create collision check).
    pub fn contains(&self, sandbox: &str) -> bool {
        self.placements.lock().unwrap().contains_key(sandbox)
    }

    /// Drop a sandbox we know is gone (e.g. after a successful kill).
    pub fn forget(&self, sandbox: &str) -> bool {
        self.placements.lock().unwrap().remove(sandbox).is_some()
    }

    /// Snapshot of every known `(sandbox, backend)` placement.
    pub fn snapshot(&self) -> Vec<(String, EndpointId)> {
        self.placements
            .lock()
            .unwrap()
            .iter()
            .map(|(s, b)| (s.clone(), *b))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.placements.lock().unwrap().len()
    }
}
