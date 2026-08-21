//! Gossip wire messages. Gossip carries only roster membership — never request
//! payloads (bidding uses direct bi-streams). Each message is a discrete gossip
//! broadcast, so we serialize straight to JSON bytes with no length framing.

use bytes::Bytes;
use iroh::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};

/// A roster membership message broadcast over the gossip topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMsg {
    /// A backend advertising itself: its dialable address (id + direct addrs)
    /// and a monotonic epoch (bumped on restart so stale announces lose).
    Announce {
        /// The backend's full address — `addr.id` is the backend id, and the
        /// direct addrs let the frontend dial without waiting on DNS discovery.
        addr: EndpointAddr,
        epoch: u64,
    },
    /// A backend leaving gracefully.
    Withdraw { id: EndpointId },
}

impl ClusterMsg {
    /// Serialize for a gossip broadcast. JSON never fails for these types.
    pub fn encode(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).expect("ClusterMsg serializes"))
    }

    /// Parse a received gossip payload.
    pub fn decode(bytes: &[u8]) -> anyhow::Result<ClusterMsg> {
        Ok(serde_json::from_slice(bytes)?)
    }
}
