//! Optional iroh-based clustering for `smolvm serve`.
//!
//! Splits the runtime into two opt-in roles that talk over iroh (QUIC p2p):
//! - **frontend**: serves the public HTTP API and dispatches each request to the
//!   backend that owns (or should own) the machine.
//! - **backend**: runs the VMs (today's engine) and answers forwarded requests +
//!   capacity bids.
//!
//! Nothing here runs unless a cluster role is explicitly enabled; the default
//! single-process `serve` path never touches this crate.

pub mod backend;
pub mod config;
pub mod frontend;
pub mod identity;
pub mod splice;
pub mod topic;

pub use backend::BackendAgent;
pub use config::{BackendConfig, FrontendConfig, LocalServe, Role, FORWARD_ALPN};
pub use identity::load_or_generate;
pub use topic::topic_from_secret;
