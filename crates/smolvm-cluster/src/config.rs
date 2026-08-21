//! Cluster configuration (roles + connection info). Inert unless a role is set.

use std::path::PathBuf;

/// ALPN for the request-forwarding protocol (frontend → backend serve socket).
pub const FORWARD_ALPN: &[u8] = b"smolvm/forward/1";

/// Which cluster role this process plays. Absent = today's single process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Runs VMs; accepts forwarded requests + (later) bids.
    Backend,
    /// Public API listener; dispatches to backends. No local VMs.
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

/// A local serve socket the backend bridges forwarded requests to. Phase 1
/// supports a Unix socket path (the backend's `-l unix://…`).
#[derive(Debug, Clone)]
pub enum LocalServe {
    Unix(PathBuf),
    Tcp(String),
}

/// Backend cluster config.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    /// Shared cluster secret (derives the gossip topic; gates membership).
    pub secret: String,
    /// The backend's own local serve socket, bridged to on forwarded requests.
    pub local_serve: LocalServe,
    /// Path to persist this node's iroh secret key.
    pub key_path: PathBuf,
}

/// Frontend cluster config.
#[derive(Debug, Clone)]
pub struct FrontendConfig {
    pub secret: String,
    /// Public address the frontend's raw HTTP listener binds.
    pub listen: String,
    /// Bootstrap backend EndpointId(s) (Phase 1: the single static backend to
    /// forward every request to).
    pub bootstrap: Vec<String>,
    pub key_path: PathBuf,
}
