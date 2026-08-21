//! Cluster configuration (roles + connection info). Inert unless a role is set.

use std::path::PathBuf;
use std::time::Duration;

/// ALPN for the single cluster connection. Backends dial the frontend on this;
/// the frontend may dial back on it to obtain a direct path. Forward requests and
/// capacity pushes are multiplexed as streams over that one connection.
pub const CLUSTER_ALPN: &[u8] = b"smolvm/cluster/1";

/// How often a backend pushes a capacity update to the frontend.
pub const CAPACITY_INTERVAL: Duration = Duration::from_secs(2);

/// Drop a backend the frontend hasn't heard a capacity push from in this long.
/// The connection closing removes it immediately; this is the backstop.
pub const BACKEND_TTL: Duration = Duration::from_secs(10);

/// Nominal memory (MiB) assumed for a create that omits `mem`, used only for the
/// placement fit gate; real admission still happens on the backend.
pub const NOMINAL_MEM_MB: u64 = 512;

/// Which cluster role this process plays. Absent = today's single process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Runs VMs; dials the frontend and serves forwarded requests.
    Backend,
    /// Public API listener; accepts backend connections and dispatches to them.
    Frontend,
}

impl Role {
    pub fn parse(s: &str) -> Option<Role> {
        match s.trim().to_ascii_lowercase().as_str() {
            "backend" => Some(Role::Backend),
            "frontend" => Some(Role::Frontend),
            _ => None,
        }
    }
}

/// A local serve socket the backend bridges forwarded requests to.
#[derive(Debug, Clone)]
pub enum LocalServe {
    Unix(PathBuf),
    Tcp(String),
}

/// Backend cluster config.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    /// Shared cluster secret; the backend proves it to the frontend on connect.
    pub secret: String,
    /// The backend's own local serve socket, bridged to on forwarded requests.
    pub local_serve: LocalServe,
    /// Path to persist this node's iroh secret key.
    pub key_path: PathBuf,
    /// The frontends' EndpointIds — the backend dials and stays connected to
    /// every one, so any of them can route to it.
    pub frontends: Vec<String>,
}

/// Frontend cluster config.
#[derive(Debug, Clone)]
pub struct FrontendConfig {
    /// Shared cluster secret; backends must present it to join.
    pub secret: String,
    /// Public address the frontend's raw HTTP listener binds.
    pub listen: String,
    pub key_path: PathBuf,
}
