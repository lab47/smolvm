//! Backend cluster agent.
//!
//! The backend dials the frontend and keeps that one connection open (redialing
//! if it drops). Over it, it pushes periodic capacity updates and serves the
//! frontend's forwarded requests — bridging each to its own local serve socket.
//! It also accepts the frontend's dial-back connection (used when the frontend
//! wants a direct path) and serves forwards on that too. No `Endpoint::connect`
//! from the frontend to the backend is ever required.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::presets;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};
use tokio::task::JoinHandle;

use crate::capacity::CapacitySource;
use crate::config::{BackendConfig, LocalServe, CAPACITY_INTERVAL, CLUSTER_ALPN};
use crate::splice::splice;
use crate::wire::{encode, membership_token, Control};

/// Accept forwarded-request bi-streams on `conn` and bridge each to the local
/// serve socket, until the connection closes.
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

/// The frontend's dial-back connections land here; serve forwards on them.
#[derive(Clone)]
struct ForwardProto {
    local: LocalServe,
}

impl std::fmt::Debug for ForwardProto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ForwardProto")
    }
}

impl ProtocolHandler for ForwardProto {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        serve_forwards(conn, self.local.clone()).await;
        Ok(())
    }
}

/// Push the membership token then periodic capacity over a control uni-stream.
async fn push_control(conn: Connection, capacity: Arc<dyn CapacitySource>, token: String) {
    let mut send = match conn.open_uni().await {
        Ok(s) => s,
        Err(_) => return,
    };
    if send
        .write_all(&encode(&Control::Hello { token }))
        .await
        .is_err()
    {
        return;
    }
    let mut tick = tokio::time::interval(CAPACITY_INTERVAL);
    loop {
        tick.tick().await;
        let snap = capacity.snapshot();
        if send
            .write_all(&encode(&Control::Capacity(snap)))
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Run one connection to the frontend until it closes: push capacity + serve
/// forwards concurrently.
async fn run_connection(conn: Connection, capacity: Arc<dyn CapacitySource>, local: LocalServe, token: String) {
    tokio::select! {
        _ = push_control(conn.clone(), capacity, token) => {}
        _ = serve_forwards(conn.clone(), local) => {}
        _ = conn.closed() => {}
    }
}

/// A running backend cluster agent.
pub struct BackendAgent {
    endpoint: Endpoint,
    router: Router,
    tasks: Vec<JoinHandle<()>>,
    id: EndpointId,
}

impl BackendAgent {
    /// Build the endpoint, start serving dial-backs, and keep a connection to the
    /// frontend (redialing on drop).
    pub async fn spawn(cfg: BackendConfig, capacity: Arc<dyn CapacitySource>) -> anyhow::Result<Self> {
        let secret = crate::identity::load_or_generate(&cfg.key_path)?;
        let builder = crate::util::apply_bind(Endpoint::builder(presets::N0).secret_key(secret))?;
        let endpoint = builder.bind().await?;
        let id = endpoint.id();

        let frontends: Vec<EndpointId> = cfg
            .frontends
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| EndpointId::from_str(s).map_err(|e| anyhow::anyhow!("invalid frontend id {s:?}: {e}")))
            .collect::<anyhow::Result<_>>()?;
        anyhow::ensure!(!frontends.is_empty(), "a backend needs at least one frontend to dial");
        let token = membership_token(&cfg.secret);
        let local = cfg.local_serve;

        let router = Router::builder(endpoint.clone())
            .accept(CLUSTER_ALPN, ForwardProto { local: local.clone() })
            .spawn();

        // One supervisor per frontend: keep a live connection to each, redialing
        // on drop. Any frontend can then route to this backend.
        let mut tasks = Vec::new();
        for frontend in frontends {
            let ep = endpoint.clone();
            let capacity = capacity.clone();
            let local = local.clone();
            let token = token.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    match ep.connect(frontend, CLUSTER_ALPN).await {
                        Ok(conn) => {
                            tracing::info!(frontend = %frontend.fmt_short(), "cluster backend: connected to frontend");
                            run_connection(conn, capacity.clone(), local.clone(), token.clone()).await;
                            tracing::info!(frontend = %frontend.fmt_short(), "cluster backend: connection closed, will redial");
                        }
                        Err(e) => {
                            tracing::debug!(frontend = %frontend.fmt_short(), error = %e, "cluster backend: dial failed");
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }

        Ok(Self {
            endpoint,
            router,
            tasks,
            id,
        })
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.id
    }

    pub async fn shutdown(self) {
        for t in &self.tasks {
            t.abort();
        }
        let _ = self.router.shutdown().await;
        self.endpoint.close().await;
    }
}
