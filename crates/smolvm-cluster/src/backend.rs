//! Backend cluster agent: an iroh endpoint that accepts forwarded requests and
//! bridges each to this node's own local serve socket.
//!
//! Phase 1 (walking skeleton): only the `smolvm/forward/1` protocol. The backend
//! runs today's `serve` engine unchanged on a local socket; forwarded requests
//! arrive on that socket exactly as if they came from loopback.

use iroh::endpoint::Connection;
use iroh::endpoint::presets;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId};

use crate::config::{BackendConfig, LocalServe, FORWARD_ALPN};
use crate::splice::splice;

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
/// call [`BackendAgent::shutdown`] to stop it cleanly.
pub struct BackendAgent {
    endpoint: Endpoint,
    router: Router,
}

impl BackendAgent {
    /// Build the iroh endpoint (persisted identity, n0 discovery) and start the
    /// forward protocol router.
    pub async fn spawn(cfg: BackendConfig) -> anyhow::Result<Self> {
        let secret = crate::identity::load_or_generate(&cfg.key_path)?;
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret)
            .bind()
            .await?;

        let router = Router::builder(endpoint.clone())
            .accept(FORWARD_ALPN, ForwardProto { local: cfg.local_serve })
            .spawn();

        Ok(Self { endpoint, router })
    }

    /// This node's stable id — configure the frontend's bootstrap with it.
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Stop the router and close the endpoint.
    pub async fn shutdown(self) {
        let _ = self.router.shutdown().await;
        self.endpoint.close().await;
    }
}
