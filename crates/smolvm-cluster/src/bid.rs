//! Capacity bidding over direct bi-streams (ALPN `smolvm/bid/1`).
//!
//! Bidding is request/response with a deadline and back-pressure, so it runs on
//! direct streams, not gossip. On each new sandbox the frontend solicits every
//! roster member in parallel; each backend replies with a score or declines.
//! One stream carries exactly one Solicit and one Bid, so we frame by EOF:
//! finish the send half after writing, read the recv half to end.

use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use serde::{Deserialize, Serialize};

use crate::capacity::{score, BidSpec, CapacitySource};

/// Max bytes accepted for a single Solicit/Bid message.
const MSG_CAP: usize = 64 * 1024;

/// Frontend → backend: score this prospective sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Solicit {
    pub spec: BidSpec,
}

/// Backend → frontend: the backend's bid (or a decline).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bid {
    /// Whether the backend is willing and able to host it.
    pub fits: bool,
    /// Higher is better. Meaningful only when `fits`.
    pub score: f64,
}

/// Backend-side bid protocol handler: answers each Solicit from local capacity.
#[derive(Clone)]
pub struct BidProto {
    capacity: Arc<dyn CapacitySource>,
}

impl std::fmt::Debug for BidProto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BidProto")
    }
}

impl BidProto {
    pub fn new(capacity: Arc<dyn CapacitySource>) -> Self {
        Self { capacity }
    }
}

impl ProtocolHandler for BidProto {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        // One solicit per bi-stream; a connection may carry several over its life.
        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let capacity = self.capacity.clone();
            tokio::spawn(async move {
                let raw = match recv.read_to_end(MSG_CAP).await {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let bid = match serde_json::from_slice::<Solicit>(&raw) {
                    Ok(sol) => {
                        let snap = capacity.snapshot();
                        match score(&snap, &sol.spec) {
                            Some(s) => Bid { fits: true, score: s },
                            None => Bid { fits: false, score: 0.0 },
                        }
                    }
                    Err(_) => Bid { fits: false, score: 0.0 },
                };
                if let Ok(bytes) = serde_json::to_vec(&bid) {
                    let _ = send.write_all(&bytes).await;
                }
                let _ = send.finish();
            });
        }
        Ok(())
    }
}

/// Ask one backend for a bid over `conn`. Returns its `Bid`, or `None` on any
/// transport error / timeout / decline.
pub async fn solicit_one(conn: &Connection, spec: BidSpec, deadline: Duration) -> Option<Bid> {
    let fut = async {
        let (mut send, mut recv) = conn.open_bi().await.ok()?;
        let body = serde_json::to_vec(&Solicit { spec }).ok()?;
        send.write_all(&body).await.ok()?;
        send.finish().ok()?;
        let raw = recv.read_to_end(MSG_CAP).await.ok()?;
        let bid: Bid = serde_json::from_slice(&raw).ok()?;
        bid.fits.then_some(bid)
    };
    tokio::time::timeout(deadline, fut).await.ok().flatten()
}
