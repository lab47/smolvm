//! The shared cluster networking layer.
//!
//! A [`ClusterNode`] owns the endpoint, the membership view, and the connections.
//! It runs the same control plane on every connection — a per-direction uni-stream
//! carrying a `Hello`, then membership announcements (and, from backends, capacity
//! pushes). The dialer keeps a live connection to every frontend the membership
//! view names; new members learned over any connection get dialed in turn.
//!
//! Who dials whom: a backend dials every frontend; a frontend dials the frontends
//! with a higher id than its own (so each frontend pair meshes with one
//! connection). Nobody dials a backend — the frontend reaches a backend over the
//! connection the backend brought.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::endpoint::{presets, Connection};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId, SecretKey};

use crate::backends::BackendTable;
use crate::capacity::CapacitySource;
use crate::config::{LocalServe, CLUSTER_ALPN, CONTROL_INTERVAL};
use crate::membership::{MemberRole, Membership};
use crate::splice::splice;
use crate::wire::{encode, read_framed, Control};

/// Role-specific state + data plane.
pub enum NodeKind {
    Frontend { backends: Arc<BackendTable> },
    Backend { local: LocalServe, capacity: Arc<dyn CapacitySource> },
}

impl NodeKind {
    fn role(&self) -> MemberRole {
        match self {
            NodeKind::Frontend { .. } => MemberRole::Frontend,
            NodeKind::Backend { .. } => MemberRole::Backend,
        }
    }
}

pub struct ClusterNode {
    endpoint: Endpoint,
    pub membership: Arc<Membership>,
    kind: NodeKind,
    token: String,
    role: MemberRole,
    dialing: Mutex<HashSet<EndpointId>>,
    router: Mutex<Option<Router>>,
}

impl ClusterNode {
    /// Build the endpoint and node. Does not start networking — call [`Self::start`].
    pub async fn build(secret: SecretKey, kind: NodeKind, token: String) -> anyhow::Result<Arc<Self>> {
        let builder = crate::util::apply_bind(Endpoint::builder(presets::N0).secret_key(secret))?;
        let endpoint = builder.bind().await?;
        let id = endpoint.id();
        let role = kind.role();
        Ok(Arc::new(Self {
            endpoint,
            membership: Arc::new(Membership::new(id, role)),
            kind,
            token,
            role,
            dialing: Mutex::new(HashSet::new()),
            router: Mutex::new(None),
        }))
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Begin accepting connections and dialing frontends, seeded with `seeds`.
    pub fn start(self: &Arc<Self>, seeds: Vec<EndpointId>) {
        self.membership.seed_frontends(seeds);

        let router = Router::builder(self.endpoint.clone())
            .accept(CLUSTER_ALPN, AcceptProto { node: self.clone() })
            .spawn();
        *self.router.lock().unwrap() = Some(router);

        // Dialer + membership sweep: keep a connection to every known frontend
        // we're not already connected to.
        let node = self.clone();
        tokio::spawn(async move {
            loop {
                for fe in node.membership.frontends() {
                    node.maybe_dial(fe);
                }
                node.membership.sweep();
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    pub async fn shutdown(&self) {
        let router = self.router.lock().unwrap().take();
        if let Some(r) = router {
            let _ = r.shutdown().await;
        }
        self.endpoint.close().await;
    }

    /// Dial `target` if we're not already connected to it and no dial is in
    /// flight. One-shot: the periodic dialer loop redials after it returns if the
    /// connection dropped and the frontend is still known.
    fn maybe_dial(self: &Arc<Self>, target: EndpointId) {
        if target == self.endpoint.id() || self.membership.is_connected(target) {
            return;
        }
        {
            let mut inflight = self.dialing.lock().unwrap();
            if !inflight.insert(target) {
                return;
            }
        }
        let node = self.clone();
        tokio::spawn(async move {
            match node.endpoint.connect(target, CLUSTER_ALPN).await {
                Ok(conn) => {
                    tracing::info!(peer = %target.fmt_short(), "cluster: connected (dialed)");
                    node.clone().handle_connection(conn).await;
                    tracing::info!(peer = %target.fmt_short(), "cluster: dialed connection closed");
                }
                Err(e) => tracing::debug!(peer = %target.fmt_short(), error = %e, "cluster: dial failed"),
            }
            node.dialing.lock().unwrap().remove(&target);
        });
    }

    /// Run the control plane (+ backend data plane) on `conn` until it closes.
    async fn handle_connection(self: Arc<Self>, conn: Connection) {
        let peer = conn.remote_id();

        let writer = {
            let node = self.clone();
            let conn = conn.clone();
            tokio::spawn(async move { node.write_control(conn).await })
        };
        let reader = {
            let node = self.clone();
            let conn = conn.clone();
            tokio::spawn(async move { node.read_control(conn, peer).await })
        };
        let forwards = match &self.kind {
            NodeKind::Backend { local, .. } => {
                let local = local.clone();
                let conn = conn.clone();
                Some(tokio::spawn(async move { serve_forwards(conn, local).await }))
            }
            NodeKind::Frontend { .. } => None,
        };

        conn.closed().await;
        writer.abort();
        reader.abort();
        if let Some(f) = forwards {
            f.abort();
        }
        self.membership.disconnected(peer);
        if let NodeKind::Frontend { backends } = &self.kind {
            backends.remove(&peer);
        }
    }

    /// Our outbound control uni-stream: Hello, then periodic membership (and
    /// capacity, if we're a backend).
    async fn write_control(self: Arc<Self>, conn: Connection) {
        let mut send = match conn.open_uni().await {
            Ok(s) => s,
            Err(_) => return,
        };
        let hello = Control::Hello {
            role: self.role,
            token: self.token.clone(),
        };
        if tokio::io::AsyncWriteExt::write_all(&mut send, &encode(&hello)).await.is_err() {
            return;
        }
        let mut tick = tokio::time::interval(CONTROL_INTERVAL);
        loop {
            tick.tick().await;
            let members = Control::Members(self.membership.announced_view());
            if tokio::io::AsyncWriteExt::write_all(&mut send, &encode(&members)).await.is_err() {
                break;
            }
            if let NodeKind::Backend { capacity, .. } = &self.kind {
                let cap = Control::Capacity(capacity.snapshot());
                if tokio::io::AsyncWriteExt::write_all(&mut send, &encode(&cap)).await.is_err() {
                    break;
                }
            }
        }
    }

    /// The peer's control uni-stream: verify Hello, then fold in membership +
    /// capacity.
    async fn read_control(self: Arc<Self>, conn: Connection, peer: EndpointId) {
        let mut recv = match conn.accept_uni().await {
            Ok(r) => r,
            Err(_) => return,
        };
        let peer_role = match read_framed::<_, Control>(&mut recv).await {
            Ok(Control::Hello { role, token }) if token == self.token => role,
            _ => return, // bad/absent token → drop
        };
        self.membership.connected(peer, peer_role);
        if let NodeKind::Frontend { backends } = &self.kind {
            if peer_role == MemberRole::Backend {
                backends.register(peer, conn.clone());
                tracing::info!(backend = %peer.fmt_short(), "cluster frontend: backend joined");
            }
        }
        loop {
            match read_framed::<_, Control>(&mut recv).await {
                Ok(Control::Members(entries)) => {
                    for fe in self.membership.merge(&entries) {
                        self.maybe_dial(fe);
                    }
                }
                Ok(Control::Capacity(cap)) => {
                    if let NodeKind::Frontend { backends } = &self.kind {
                        backends.update_capacity(peer, cap);
                    }
                }
                Ok(Control::Hello { .. }) => {}
                Err(_) => break,
            }
        }
    }
}

struct AcceptProto {
    node: Arc<ClusterNode>,
}

impl std::fmt::Debug for AcceptProto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AcceptProto")
    }
}

impl ProtocolHandler for AcceptProto {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.node.clone().handle_connection(conn).await;
        Ok(())
    }
}

/// Accept forward bi-streams on `conn` and bridge each to the local serve socket.
async fn serve_forwards(conn: Connection, local: LocalServe) {
    loop {
        let (send, recv) = match conn.accept_bi().await {
            Ok(pair) => pair,
            Err(_) => break,
        };
        let local = local.clone();
        tokio::spawn(async move {
            match &local {
                LocalServe::Unix(path) => match tokio::net::UnixStream::connect(path).await {
                    Ok(s) => splice(s, send, recv).await,
                    Err(e) => tracing::warn!(error = %e, "backend: cannot reach local serve socket"),
                },
                LocalServe::Tcp(addr) => match tokio::net::TcpStream::connect(addr).await {
                    Ok(s) => splice(s, send, recv).await,
                    Err(e) => tracing::warn!(error = %e, "backend: cannot reach local serve socket"),
                },
            }
        });
    }
}
