//! The capacity boundary between the cluster transport and the serve engine.
//!
//! A backend scores each solicited sandbox from cheap local signals. The scoring
//! lives here (shared, testable); the *signals* come from whatever implements
//! [`CapacitySource`] — in practice `ApiState` in `src/api`, which keeps iroh out
//! of the main crate and this crate off the full `smolvm` crate.

use serde::{Deserialize, Serialize};

/// A point-in-time view of a backend's spare capacity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CapacitySnapshot {
    /// Host memory available right now, in MiB (`/proc/meminfo` MemAvailable).
    pub mem_available_mb: u64,
    /// VMs currently running on this backend.
    pub running_vms: u32,
    /// vCPU-equivalents actively in use across this backend's VMs.
    pub cpu_used: f64,
    /// The runtime heartbeat has gone stale — the node is unhealthy; don't bid.
    pub stalled: bool,
}

/// What a new sandbox is expected to need.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BidSpec {
    pub mem_mb: u64,
    pub vcpus: u32,
}

/// Supplies live capacity signals. Implemented by the serve engine.
pub trait CapacitySource: Send + Sync {
    fn snapshot(&self) -> CapacitySnapshot;
}

/// Score a solicited sandbox. `None` means "don't bid" (can't fit / unhealthy);
/// otherwise higher is better. Memory headroom dominates — it's the binding
/// constraint for microVMs — with running-VM count and CPU load as spread
/// tie-breakers so equal-RAM nodes fill evenly.
pub fn score(snap: &CapacitySnapshot, spec: &BidSpec) -> Option<f64> {
    if snap.stalled {
        return None;
    }
    if spec.mem_mb > snap.mem_available_mb {
        return None; // hard reject: won't fit
    }
    let mem_headroom = (snap.mem_available_mb - spec.mem_mb) as f64;
    let vm_penalty = snap.running_vms as f64 * 512.0;
    let cpu_penalty = snap.cpu_used * 256.0;
    Some(mem_headroom - vm_penalty - cpu_penalty)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(mem: u64, vms: u32) -> CapacitySnapshot {
        CapacitySnapshot {
            mem_available_mb: mem,
            running_vms: vms,
            cpu_used: 0.0,
            stalled: false,
        }
    }

    #[test]
    fn no_bid_when_stalled_or_too_big() {
        let spec = BidSpec { mem_mb: 1024, vcpus: 2 };
        let mut s = snap(8192, 0);
        s.stalled = true;
        assert!(score(&s, &spec).is_none());
        assert!(score(&snap(512, 0), &spec).is_none()); // 1024 > 512
    }

    #[test]
    fn more_free_ram_scores_higher_and_vms_break_ties() {
        let spec = BidSpec { mem_mb: 1024, vcpus: 2 };
        assert!(score(&snap(16384, 0), &spec) > score(&snap(8192, 0), &spec));
        // same RAM, fewer running VMs wins
        assert!(score(&snap(8192, 1), &spec) > score(&snap(8192, 4), &spec));
    }
}
