//! Frontend cluster agent: the public HTTP listener. It joins the gossip topic
//! as the seed, builds a live roster of backends from their announcements, and
//! tunnels each request over an iroh bi-stream to a backend's local serve socket.
//!
//! Phase 2: the roster is dynamic (gossip), but placement is still trivial — every
//! request goes to one deterministic backend (lowest id) so multi-request flows
//! stay on the machine's owner. Bidding (Phase 3) and by-name routing (Phase 4)
//! replace that. Two paths are served locally instead of tunneled: `GET /health`
//! (frontend liveness) and `GET /cluster/roster` (introspection).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures_lite::StreamExt;
use iroh::endpoint::{presets, Connection};
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_gossip::api::Event;
use iroh_gossip::net::Gossip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::config::{FrontendConfig, FORWARD_ALPN};
use crate::roster::{Roster, ANNOUNCE_INTERVAL};
use crate::splice::splice_with_preamble;
use crate::topic::topic_from_secret;
use crate::util::parse_ids;
use crate::wire::ClusterMsg;

/// Cap on how much of a request we read while peeking the request line.
const HEAD_PEEK_CAP: usize = 8192;

/// The frontend's shared routing state.
struct Frontend {
    endpoint: Endpoint,
    roster: Arc<Roster>,
    /// Cached forward connections, one per backend.
    conns: Mutex<HashMap<EndpointId, Connection>>,
}

impl Frontend {
    /// Deterministic Phase-2 placement: the live backend with the lowest id.
    fn pick(&self) -> Option<(EndpointId, EndpointAddr)> {
        let mut backends = self.roster.snapshot();
        backends.sort_by_key(|b| b.id.as_bytes().to_owned());
        backends.into_iter().next().map(|b| (b.id, b.addr))
    }

    /// A live forward connection to `id`, redialing if the cached one is gone.
    async fn connection(&self, id: EndpointId, addr: EndpointAddr) -> anyhow::Result<Connection> {
        let mut guard = self.conns.lock().await;
        if let Some(c) = guard.get(&id) {
            if c.close_reason().is_none() {
                return Ok(c.clone());
            }
        }
        let c = self.endpoint.connect(addr, FORWARD_ALPN).await?;
        guard.insert(id, c.clone());
        Ok(c)
    }

    async fn invalidate(&self, id: &EndpointId) {
        self.conns.lock().await.remove(id);
    }
}

/// Read the request line (and whatever else arrives in the same reads) without
/// consuming past what we buffer — the buffer is replayed into the tunnel. Returns
/// `(buffered_bytes, method, path)`.
async fn peek_head(client: &mut TcpStream) -> std::io::Result<(Vec<u8>, String, String)> {
    let mut buf = Vec::with_capacity(256);
    let mut tmp = [0u8; 1024];
    loop {
        if buf.contains(&b'\n') || buf.len() >= HEAD_PEEK_CAP {
            break;
        }
        let n = client.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let line_end = buf.iter().position(|&b| b == b'\n').unwrap_or(buf.len());
    let line = String::from_utf8_lossy(&buf[..line_end]);
    let mut parts = line.trim_end_matches('\r').split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    Ok((buf, method, path))
}

/// Write a small JSON response and close (Connection: close).
async fn respond_json(client: &mut TcpStream, status: &str, body: String) {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    let _ = client.write_all(resp.as_bytes()).await;
    let _ = client.shutdown().await;
}

fn roster_json(roster: &Roster) -> String {
    let backends: Vec<serde_json::Value> = roster
        .snapshot()
        .into_iter()
        .map(|b| {
            let addrs: Vec<String> = b
                .addr
                .addrs
                .iter()
                .map(|a| a.to_string())
                .collect();
            serde_json::json!({
                "id": b.id.to_string(),
                "epoch": b.epoch,
                "age_ms": b.age.as_millis() as u64,
                "addrs": addrs,
            })
        })
        .collect();
    serde_json::json!({ "count": backends.len(), "backends": backends }).to_string()
}

/// Handle one accepted client connection.
async fn serve_client(fe: Arc<Frontend>, mut client: TcpStream) {
    let (buf, method, path) = match peek_head(&mut client).await {
        Ok(v) => v,
        Err(_) => return,
    };
    let route = path.split('?').next().unwrap_or("");

    // Frontend-owned endpoints answer locally instead of tunneling.
    if method == "GET" && route == "/health" {
        let body = serde_json::json!({
            "status": "ok",
            "role": "frontend",
            "backends": fe.roster.len(),
        })
        .to_string();
        respond_json(&mut client, "200 OK", body).await;
        return;
    }
    if method == "GET" && route == "/cluster/roster" {
        respond_json(&mut client, "200 OK", roster_json(&fe.roster)).await;
        return;
    }

    // Everything else tunnels to a backend.
    let Some((id, addr)) = fe.pick() else {
        respond_json(
            &mut client,
            "503 Service Unavailable",
            serde_json::json!({"error": "no backend available"}).to_string(),
        )
        .await;
        return;
    };

    for attempt in 0..2 {
        let conn = match fe.connection(id, addr.clone()).await {
            Ok(c) => c,
            Err(e) => {
                if attempt == 0 {
                    fe.invalidate(&id).await;
                    continue;
                }
                tracing::warn!(backend = %id.fmt_short(), error = %e, "frontend: cannot reach backend");
                return;
            }
        };
        match conn.open_bi().await {
            Ok((send, recv)) => {
                splice_with_preamble(client, send, recv, buf).await;
                return;
            }
            Err(_) if attempt == 0 => {
                fe.invalidate(&id).await;
                continue;
            }
            Err(e) => {
                tracing::warn!(backend = %id.fmt_short(), error = %e, "frontend: cannot open stream");
                return;
            }
        }
    }
}

/// Run the frontend until `shutdown` resolves.
pub async fn run(cfg: FrontendConfig, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
    let secret = crate::identity::load_or_generate(&cfg.key_path)?;
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret)
        .bind()
        .await?;
    let id = endpoint.id();

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    let topic = topic_from_secret(&cfg.secret);
    let extra_peers = parse_ids(&cfg.bootstrap)?;
    let (_gossip_send, mut gossip_recv) = gossip.subscribe(topic, extra_peers).await?.split();

    let roster = Arc::new(Roster::new());

    // Roster updater: fold gossip announcements into the roster.
    let roster_up = roster.clone();
    let updater = tokio::spawn(async move {
        while let Some(event) = gossip_recv.next().await {
            match event {
                Ok(Event::Received(msg)) => match ClusterMsg::decode(&msg.content) {
                    Ok(ClusterMsg::Announce { addr, epoch }) => {
                        let id = addr.id;
                        let known = roster_up.snapshot().iter().any(|b| b.id == id);
                        roster_up.upsert(addr, epoch);
                        if !known {
                            tracing::info!(backend = %id.fmt_short(), "cluster frontend: backend joined roster");
                        }
                    }
                    Ok(ClusterMsg::Withdraw { id }) => {
                        if roster_up.remove(&id) {
                            tracing::info!(backend = %id.fmt_short(), "cluster frontend: backend withdrew");
                        }
                    }
                    Err(e) => tracing::debug!(error = %e, "frontend: bad gossip message"),
                },
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(error = %e, "frontend: gossip stream error");
                    break;
                }
            }
        }
    });

    // TTL sweep: evict backends that stopped announcing.
    let roster_sweep = roster.clone();
    let sweeper = tokio::spawn(async move {
        let mut tick = tokio::time::interval(ANNOUNCE_INTERVAL);
        loop {
            tick.tick().await;
            for id in roster_sweep.sweep() {
                tracing::info!(backend = %id.fmt_short(), "cluster frontend: backend evicted (TTL)");
            }
        }
    });

    let fe = Arc::new(Frontend {
        endpoint: endpoint.clone(),
        roster,
        conns: Mutex::new(HashMap::new()),
    });

    let listener = TcpListener::bind(&cfg.listen).await?;
    println!("cluster frontend endpoint id: {id}");
    tracing::info!(listen = %cfg.listen, endpoint_id = %id, "cluster frontend listening (gossip roster)");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((client, _peer)) => {
                        let fe = fe.clone();
                        tokio::spawn(serve_client(fe, client));
                    }
                    Err(e) => tracing::warn!(error = %e, "cluster frontend: accept failed"),
                }
            }
        }
    }

    updater.abort();
    sweeper.abort();
    let _ = router.shutdown().await;
    endpoint.close().await;
    Ok(())
}
