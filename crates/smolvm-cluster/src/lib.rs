//! Optional iroh-based clustering for `smolvm serve`.
//!
//! Splits the runtime into two opt-in roles that talk over one iroh connection
//! per pair:
//! - **frontend**: serves the public HTTP API, accepts backend connections, and
//!   dispatches each request over the connection the backend brought.
//! - **backend**: runs the VMs (today's engine), dials the frontend, serves
//!   forwarded requests, and pushes capacity for placement.
//!
//! Backends dial the frontend (the direction that survives NAT); the frontend
//! reuses those connections bidirectionally and dials back only to upgrade a
//! relay path to a direct one. Nothing here runs unless a role is enabled.

pub mod backend;
pub mod backends;
pub mod capacity;
pub mod config;
pub mod frontend;
pub mod http;
pub mod identity;
pub mod link;
pub mod membership;
pub mod registry;
pub mod splice;
pub mod wire;
mod util;

pub use backend::BackendAgent;
pub use capacity::{score, BidSpec, CapacitySnapshot, CapacitySource};
pub use config::{BackendConfig, FrontendConfig, LocalServe, Role, CLUSTER_ALPN};
pub use identity::load_or_generate;
