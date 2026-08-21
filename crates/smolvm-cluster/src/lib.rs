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

pub mod identity;
pub mod topic;

pub use identity::load_or_generate;
pub use topic::topic_from_secret;
