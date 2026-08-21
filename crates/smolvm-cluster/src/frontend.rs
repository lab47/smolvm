//! Frontend cluster agent: a raw TCP listener that tunnels every accepted
//! connection over an iroh bi-stream to a backend's local serve socket.
//!
//! Phase 1 (walking skeleton): static routing — every request goes to the single
//! configured bootstrap backend. No HTTP parsing happens here; bytes flow through
//! untouched, which is what preserves SSE, WebSocket upgrades, and large binary
//! file transfers. Gossip roster + bidding + by-name routing come in later phases.

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointId};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::config::{FrontendConfig, FORWARD_ALPN};
use crate::splice::splice;

/// Shared connection to the static backend, reconnected on demand.
struct Link {
    endpoint: Endpoint,
    backend: EndpointId,
    conn: Mutex<Option<Connection>>,
}

impl Link {
    /// Return a live connection, reconnecting if the cached one is gone.
    async fn connection(&self) -> anyhow::Result<Connection> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            if c.close_reason().is_none() {
                return Ok(c.clone());
            }
        }
        let c = self.endpoint.connect(self.backend, FORWARD_ALPN).await?;
        *guard = Some(c.clone());
        Ok(c)
    }

    /// Drop the cached connection so the next call redials.
    async fn invalidate(&self) {
        *self.conn.lock().await = None;
    }
}

/// Tunnel one accepted client socket to the backend, redialing once if the
/// cached QUIC connection has gone away.
async fn tunnel(link: Arc<Link>, client: tokio::net::TcpStream) {
    for attempt in 0..2 {
        let conn = match link.connection().await {
            Ok(c) => c,
            Err(e) => {
                if attempt == 0 {
                    link.invalidate().await;
                    continue;
                }
                tracing::warn!(error = %e, "cluster frontend: cannot reach backend");
                return;
            }
        };
        match conn.open_bi().await {
            Ok((send, recv)) => {
                splice(client, send, recv).await;
                return;
            }
            Err(_) if attempt == 0 => {
                link.invalidate().await;
                continue;
            }
            Err(e) => {
                tracing::warn!(error = %e, "cluster frontend: cannot open stream to backend");
                return;
            }
        }
    }
}

/// Run the frontend until `shutdown` resolves.
pub async fn run(
    cfg: FrontendConfig,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let backend_str = cfg
        .bootstrap
        .first()
        .ok_or_else(|| anyhow::anyhow!("cluster frontend requires a bootstrap backend id"))?;
    let backend = EndpointId::from_str(backend_str.trim())
        .map_err(|e| anyhow::anyhow!("invalid bootstrap backend id {backend_str:?}: {e}"))?;

    let secret = crate::identity::load_or_generate(&cfg.key_path)?;
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret)
        .bind()
        .await?;

    let endpoint_for_close = endpoint.clone();
    let link = Arc::new(Link {
        endpoint,
        backend,
        conn: Mutex::new(None),
    });

    let listener = TcpListener::bind(&cfg.listen).await?;
    tracing::info!(
        listen = %cfg.listen,
        backend = %backend.fmt_short(),
        "cluster frontend listening (static forward)"
    );

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((client, _peer)) => {
                        let link = link.clone();
                        tokio::spawn(tunnel(link, client));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "cluster frontend: accept failed");
                    }
                }
            }
        }
    }

    drop(link);
    endpoint_for_close.close().await;
    Ok(())
}
