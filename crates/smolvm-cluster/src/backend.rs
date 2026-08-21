//! Backend cluster agent: an iroh endpoint that accepts forwarded requests,
//! bridges each to this node's own local serve socket, and announces itself on
//! the gossip topic so the frontend can discover and route to it.

use std::sync::Arc;

use futures_lite::StreamExt;
use iroh::endpoint::presets;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};
use iroh_gossip::api::Event;
use iroh_gossip::net::Gossip;
use tokio::task::JoinHandle;

use crate::bid::BidProto;
use crate::capacity::CapacitySource;
use crate::config::{BackendConfig, LocalServe, BID_ALPN, FORWARD_ALPN};
use crate::roster::ANNOUNCE_INTERVAL;
use crate::splice::splice;
use crate::topic::topic_from_secret;
use crate::util::{now_millis, parse_ids};
use crate::wire::ClusterMsg;

/// Bridges inbound forward streams to the backend's local serve socket.
#[derive(Debug, Clone)]
struct ForwardProto {
    local: LocalServe,
}

impl ProtocolHandler for ForwardProto {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        // A single QUIC connection carries one bi-stream per forwarded HTTP
        // request. Loop until the peer closes the connection.
        loop {
            let (send, recv) = match conn.accept_bi().await {
                Ok(pair) => pair,
                Err(_) => break, // peer closed the connection — normal
            };
            let local = self.local.clone();
            tokio::spawn(async move {
                match &local {
                    LocalServe::Unix(path) => match tokio::net::UnixStream::connect(path).await {
                        Ok(s) => splice(s, send, recv).await,
                        Err(e) => tracing::warn!(
                            error = %e,
                            path = %path.display(),
                            "cluster backend: could not reach local serve socket"
                        ),
                    },
                    LocalServe::Tcp(addr) => match tokio::net::TcpStream::connect(addr).await {
                        Ok(s) => splice(s, send, recv).await,
                        Err(e) => tracing::warn!(
                            error = %e,
                            addr = %addr,
                            "cluster backend: could not reach local serve socket"
                        ),
                    },
                }
            });
        }
        Ok(())
    }
}

/// A running backend cluster agent. Hold it for the lifetime of the process;
/// call [`BackendAgent::shutdown`] to stop it (broadcasts a graceful Withdraw).
pub struct BackendAgent {
    endpoint: Endpoint,
    router: Router,
    gossip_send: iroh_gossip::api::GossipSender,
    id: EndpointId,
    tasks: Vec<JoinHandle<()>>,
}

impl BackendAgent {
    /// Build the iroh endpoint (persisted identity, n0 discovery), start the
    /// forward + bid + gossip router, join the topic, and begin announcing.
    pub async fn spawn(
        cfg: BackendConfig,
        capacity: Arc<dyn CapacitySource>,
    ) -> anyhow::Result<Self> {
        let secret = crate::identity::load_or_generate(&cfg.key_path)?;
        let builder = crate::util::apply_bind(Endpoint::builder(presets::N0).secret_key(secret))?;
        let endpoint = builder.bind().await?;
        let id = endpoint.id();

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(FORWARD_ALPN, ForwardProto { local: cfg.local_serve })
            .accept(BID_ALPN, BidProto::new(capacity))
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        let topic = topic_from_secret(&cfg.secret);
        let bootstrap = parse_ids(&cfg.bootstrap)?;
        let (gossip_send, gossip_recv) = gossip.subscribe(topic, bootstrap).await?.split();

        let epoch = now_millis();
        let mut tasks = Vec::new();

        // Announce loop: advertise our dialable address on the topic. The first
        // interval tick fires immediately, so the frontend sees us quickly.
        //
        // Use broadcast_neighbors, NOT broadcast: an announce is a periodic
        // heartbeat to our direct neighbors (the frontend), not something to
        // disseminate multi-hop. broadcast() runs Plumtree, which prunes a node
        // to lazy-push after the first message and then only sends IHAVE digests,
        // relying on a GRAFT pull that doesn't reliably complete over a relay hop
        // — so the frontend would stop hearing announces and evict us. Sending
        // the full message to neighbors every tick avoids that entirely.
        let announce_ep = endpoint.clone();
        let announce_send = gossip_send.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(ANNOUNCE_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let msg = ClusterMsg::Announce {
                    addr: announce_ep.addr(),
                    epoch,
                };
                if let Err(e) = announce_send.broadcast_neighbors(msg.encode()).await {
                    tracing::debug!(error = %e, "cluster backend: announce broadcast failed");
                }
            }
        }));

        // Drain the gossip event stream (log neighbor churn). Backends don't keep
        // a roster; they only need to stay subscribed so the swarm forwards.
        tasks.push(tokio::spawn(async move {
            let mut recv = gossip_recv;
            while let Some(event) = recv.next().await {
                match event {
                    Ok(Event::NeighborUp(peer)) => {
                        tracing::debug!(peer = %peer.fmt_short(), "cluster backend: neighbor up")
                    }
                    Ok(Event::NeighborDown(peer)) => {
                        tracing::debug!(peer = %peer.fmt_short(), "cluster backend: neighbor down")
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::debug!(error = %e, "cluster backend: gossip stream error");
                        break;
                    }
                }
            }
        }));

        Ok(Self {
            endpoint,
            router,
            gossip_send,
            id,
            tasks,
        })
    }

    /// This node's stable id.
    pub fn endpoint_id(&self) -> EndpointId {
        self.id
    }

    /// Broadcast a graceful Withdraw, stop the router, and close the endpoint.
    pub async fn shutdown(self) {
        let withdraw = ClusterMsg::Withdraw { id: self.id };
        let _ = self.gossip_send.broadcast_neighbors(withdraw.encode()).await;
        for t in &self.tasks {
            t.abort();
        }
        let _ = self.router.shutdown().await;
        self.endpoint.close().await;
    }
}
