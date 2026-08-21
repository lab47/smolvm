//! Frontend cluster agent: the public HTTP listener. It joins the gossip topic as
//! the seed, builds a live roster of backends, and routes each request:
//!
//! - `GET /health`, `GET /cluster/roster` — answered locally.
//! - `POST /api/v1/machines` (create) — solicit bids from every backend, assign
//!   the machine a name, and place it on the best bidder (retrying the next on a
//!   capacity failure); record `name → backend`.
//! - `/api/v1/machines/{name}/…` — tunnel to the recorded owner (fallback: lowest
//!   id until Phase 4 adds a fan-out probe).
//! - anything else — tunnel to the lowest-id backend.
//!
//! Streaming bodies (SSE, WS, file transfer) are never buffered: only the create
//! path reads a body, and create bodies are small JSON.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use iroh::endpoint::{presets, Connection};
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_gossip::api::Event;
use iroh_gossip::net::Gossip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::bid::{self, Bid};
use crate::capacity::BidSpec;
use crate::config::{FrontendConfig, BID_ALPN, DEFAULT_BID_MS, FORWARD_ALPN};
use crate::http::{self, ReqHead};
use crate::roster::{Roster, ANNOUNCE_INTERVAL};
use crate::splice::splice_with_preamble;
use crate::topic::topic_from_secret;
use crate::util::{now_millis, parse_ids};
use crate::wire::ClusterMsg;

/// Cap on a buffered create response.
const RESP_CAP: usize = 4 * 1024 * 1024;
/// Nominal memory (MiB) assumed for a create that omits `mem`, used only for the
/// bid hard-reject gate; real admission still happens on the backend.
const NOMINAL_MEM_MB: u64 = 512;

/// The frontend's shared routing state.
struct Frontend {
    endpoint: Endpoint,
    roster: Arc<Roster>,
    /// Forward (request-tunnel) connections, one per backend.
    fwd_conns: Mutex<HashMap<EndpointId, Connection>>,
    /// Bid connections, one per backend (separate ALPN → separate connection).
    bid_conns: Mutex<HashMap<EndpointId, Connection>>,
    /// Routing table: machine name → owning backend.
    table: Mutex<HashMap<String, EndpointId>>,
    bid_deadline: Duration,
    name_ctr: AtomicU64,
}

impl Frontend {
    /// The live backend with the lowest id — the deterministic fallback target.
    fn lowest(&self) -> Option<(EndpointId, EndpointAddr)> {
        let mut backends = self.roster.snapshot();
        backends.sort_by_key(|b| b.id.as_bytes().to_owned());
        backends.into_iter().next().map(|b| (b.id, b.addr))
    }

    /// Look up a backend's current address in the roster.
    fn addr_of(&self, id: EndpointId) -> Option<EndpointAddr> {
        self.roster.snapshot().into_iter().find(|b| b.id == id).map(|b| b.addr)
    }

    async fn cached_connection(
        &self,
        cache: &Mutex<HashMap<EndpointId, Connection>>,
        alpn: &[u8],
        id: EndpointId,
        addr: EndpointAddr,
    ) -> anyhow::Result<Connection> {
        let mut guard = cache.lock().await;
        if let Some(c) = guard.get(&id) {
            if c.close_reason().is_none() {
                return Ok(c.clone());
            }
        }
        let c = self.endpoint.connect(addr, alpn).await?;
        guard.insert(id, c.clone());
        Ok(c)
    }

    async fn fwd_connection(&self, id: EndpointId, addr: EndpointAddr) -> anyhow::Result<Connection> {
        self.cached_connection(&self.fwd_conns, FORWARD_ALPN, id, addr).await
    }

    async fn bid_connection(&self, id: EndpointId, addr: EndpointAddr) -> anyhow::Result<Connection> {
        self.cached_connection(&self.bid_conns, BID_ALPN, id, addr).await
    }

    async fn invalidate_fwd(&self, id: &EndpointId) {
        self.fwd_conns.lock().await.remove(id);
    }

    /// A native-looking unique machine name.
    fn gen_name(&self) -> String {
        let n = now_millis();
        let c = self.name_ctr.fetch_add(1, Ordering::Relaxed);
        let mut seed = Vec::with_capacity(16);
        seed.extend_from_slice(&n.to_le_bytes());
        seed.extend_from_slice(&c.to_le_bytes());
        let h = blake3::hash(&seed);
        let b = &h.as_bytes()[..4];
        format!("vm-{:02x}{:02x}{:02x}{:02x}", b[0], b[1], b[2], b[3])
    }
}

/// Solicit bids from every roster backend in parallel; return `(id, addr, score)`
/// sorted best-first.
async fn run_bids(fe: &Arc<Frontend>, spec: BidSpec) -> Vec<(EndpointId, EndpointAddr, f64)> {
    let mut set: JoinSet<Option<(EndpointId, EndpointAddr, f64)>> = JoinSet::new();
    for b in fe.roster.snapshot() {
        let fe = fe.clone();
        set.spawn(async move {
            let conn = fe.bid_connection(b.id, b.addr.clone()).await.ok()?;
            let Bid { score, .. } = bid::solicit_one(&conn, spec, fe.bid_deadline).await?;
            Some((b.id, b.addr, score))
        });
    }
    let mut out = Vec::new();
    while let Some(res) = set.join_next().await {
        if let Ok(Some(t)) = res {
            out.push(t);
        }
    }
    out.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
    out
}

/// Buffered request/response over a fresh bi-stream. The request carries
/// `Connection: close`, so the backend closes after responding, giving us EOF to
/// read the whole response. We must NOT finish the send half before reading — an
/// early FIN makes the backend's HTTP stack drop the in-flight response.
async fn forward_buffered(conn: &Connection, request: &[u8]) -> anyhow::Result<(u16, Vec<u8>)> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(request).await?;
    let resp = tokio::time::timeout(Duration::from_secs(30), recv.read_to_end(RESP_CAP))
        .await
        .map_err(|_| anyhow::anyhow!("timed out reading response"))?
        .map_err(|e| anyhow::anyhow!("read response: {e}"))?;
    let _ = send.finish();
    let status = http::parse_status(&resp)
        .ok_or_else(|| anyhow::anyhow!("no status line ({} bytes)", resp.len()))?;
    Ok((status, resp))
}

/// Write a small JSON response and close.
async fn respond_json(client: &mut TcpStream, status: &str, body: String) {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    let _ = client.write_all(resp.as_bytes()).await;
    let _ = client.shutdown().await;
}

fn roster_json(fe: &Frontend) -> String {
    let backends: Vec<serde_json::Value> = fe
        .roster
        .snapshot()
        .into_iter()
        .map(|b| {
            let addrs: Vec<String> = b.addr.addrs.iter().map(|a| a.to_string()).collect();
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

/// Assign a name to the create body (honoring a client-supplied one) and derive
/// the bid spec. Returns `(rewritten_body, name)`, or `None` if the body isn't a
/// JSON object we can place.
fn prepare_create(body: &[u8], fe: &Frontend) -> Option<(Vec<u8>, String, BidSpec)> {
    let mut val: serde_json::Value = serde_json::from_slice(body).ok()?;
    let obj = val.as_object_mut()?;
    let name = match obj.get("name").and_then(|v| v.as_str()) {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            let n = fe.gen_name();
            obj.insert("name".to_string(), serde_json::Value::String(n.clone()));
            n
        }
    };
    let mem_mb = obj.get("mem").and_then(|v| v.as_u64()).unwrap_or(NOMINAL_MEM_MB);
    let vcpus = obj.get("cpus").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let rewritten = serde_json::to_vec(&val).ok()?;
    Some((rewritten, name, BidSpec { mem_mb, vcpus }))
}

/// Handle `POST /api/v1/machines`: bid, place, record.
async fn create_flow(
    fe: Arc<Frontend>,
    mut client: TcpStream,
    head: ReqHead,
    mut buf: Vec<u8>,
    body_start: usize,
) {
    // Read the full body (Content-Length).
    let want = head.content_length().unwrap_or(0);
    let mut body = buf.split_off(body_start);
    let mut tmp = [0u8; 4096];
    while body.len() < want {
        match client.read(&mut tmp).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&tmp[..n]),
        }
    }
    if want > 0 {
        body.truncate(want);
    }

    let Some((new_body, name, spec)) = prepare_create(&body, &fe) else {
        respond_json(
            &mut client,
            "400 Bad Request",
            serde_json::json!({"error": "cluster frontend could not parse create body"}).to_string(),
        )
        .await;
        return;
    };

    let bidders = run_bids(&fe, spec).await;
    tracing::info!(machine = %name, bidders = bidders.len(), "cluster: bid round complete");
    if bidders.is_empty() {
        respond_json(
            &mut client,
            "503 Service Unavailable",
            serde_json::json!({"error": "no backend bid on this sandbox"}).to_string(),
        )
        .await;
        return;
    }

    let request = http::build_request(&head, &new_body);
    let mut last: Option<Vec<u8>> = None;
    for (id, addr, score) in bidders.into_iter().take(3) {
        let conn = match fe.fwd_connection(id, addr).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(backend = %id.fmt_short(), error = %e, "create: forward connect failed");
                fe.invalidate_fwd(&id).await;
                continue;
            }
        };
        match forward_buffered(&conn, &request).await {
            Ok((status, resp)) if status < 500 && status != 409 => {
                if status < 300 {
                    fe.table.lock().await.insert(name.clone(), id);
                    tracing::info!(machine = %name, backend = %id.fmt_short(), score, "cluster: placed sandbox");
                }
                let _ = client.write_all(&resp).await;
                let _ = client.shutdown().await;
                return;
            }
            Ok((status, resp)) => {
                tracing::debug!(backend = %id.fmt_short(), status, "cluster: bidder rejected create, trying next");
                last = Some(resp);
            }
            Err(e) => {
                tracing::warn!(backend = %id.fmt_short(), error = %e, "create: forward exchange failed");
                fe.invalidate_fwd(&id).await;
            }
        }
    }

    // Every bidder rejected — relay the last backend response, or a 503.
    match last {
        Some(resp) => {
            let _ = client.write_all(&resp).await;
            let _ = client.shutdown().await;
        }
        None => {
            respond_json(
                &mut client,
                "503 Service Unavailable",
                serde_json::json!({"error": "all backends failed to place sandbox"}).to_string(),
            )
            .await;
        }
    }
}

/// Tunnel the client connection to `target` (streaming, preamble = bytes already
/// read). Redials once if the cached connection is gone.
async fn tunnel_to(fe: Arc<Frontend>, client: TcpStream, id: EndpointId, addr: EndpointAddr, preamble: Vec<u8>) {
    for attempt in 0..2 {
        let conn = match fe.fwd_connection(id, addr.clone()).await {
            Ok(c) => c,
            Err(e) => {
                if attempt == 0 {
                    fe.invalidate_fwd(&id).await;
                    continue;
                }
                tracing::warn!(backend = %id.fmt_short(), error = %e, "frontend: cannot reach backend");
                return;
            }
        };
        match conn.open_bi().await {
            Ok((send, recv)) => {
                splice_with_preamble(client, send, recv, preamble).await;
                return;
            }
            Err(_) if attempt == 0 => fe.invalidate_fwd(&id).await,
            Err(e) => {
                tracing::warn!(backend = %id.fmt_short(), error = %e, "frontend: cannot open stream");
                return;
            }
        }
    }
}

/// Handle one accepted client connection.
async fn serve_client(fe: Arc<Frontend>, mut client: TcpStream) {
    let mut buf = Vec::with_capacity(512);
    let body_start = match http::read_headers(&mut client, &mut buf).await {
        Ok(Some(pos)) => pos,
        _ => return,
    };
    let Some(head) = http::parse_head(&buf[..body_start]) else {
        return;
    };
    let route = head.path().to_string();
    let method = head.method.clone();

    // Frontend-owned endpoints.
    if method == "GET" && route == "/health" {
        let body = serde_json::json!({
            "status": "ok", "role": "frontend", "backends": fe.roster.len(),
        })
        .to_string();
        respond_json(&mut client, "200 OK", body).await;
        return;
    }
    if method == "GET" && route == "/cluster/roster" {
        respond_json(&mut client, "200 OK", roster_json(&fe)).await;
        return;
    }

    // Create: bid + place.
    if method == "POST" && route == "/api/v1/machines" {
        create_flow(fe, client, head, buf, body_start).await;
        return;
    }

    // Choose a backend to tunnel to: by-name owner, else lowest-id fallback.
    let target = if let Some(rest) = route.strip_prefix("/api/v1/machines/") {
        let name = rest.split('/').next().unwrap_or("");
        let owner = fe.table.lock().await.get(name).copied();
        match owner.and_then(|id| fe.addr_of(id).map(|a| (id, a))) {
            Some(t) => Some(t),
            None => fe.lowest(),
        }
    } else {
        fe.lowest()
    };

    let Some((id, addr)) = target else {
        respond_json(
            &mut client,
            "503 Service Unavailable",
            serde_json::json!({"error": "no backend available"}).to_string(),
        )
        .await;
        return;
    };
    tunnel_to(fe, client, id, addr, buf).await;
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

    let roster_up = roster.clone();
    let updater = tokio::spawn(async move {
        while let Some(event) = gossip_recv.next().await {
            match event {
                Ok(Event::Received(msg)) => match ClusterMsg::decode(&msg.content) {
                    Ok(ClusterMsg::Announce { addr, epoch }) => {
                        let bid = addr.id;
                        let known = roster_up.snapshot().iter().any(|b| b.id == bid);
                        roster_up.upsert(addr, epoch);
                        if !known {
                            tracing::info!(backend = %bid.fmt_short(), "cluster frontend: backend joined roster");
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

    let bid_ms = std::env::var("SMOLVM_CLUSTER_BID_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BID_MS);

    let fe = Arc::new(Frontend {
        endpoint: endpoint.clone(),
        roster,
        fwd_conns: Mutex::new(HashMap::new()),
        bid_conns: Mutex::new(HashMap::new()),
        table: Mutex::new(HashMap::new()),
        bid_deadline: Duration::from_millis(bid_ms),
        name_ctr: AtomicU64::new(0),
    });

    let listener = TcpListener::bind(&cfg.listen).await?;
    println!("cluster frontend endpoint id: {id}");
    tracing::info!(listen = %cfg.listen, endpoint_id = %id, bid_ms, "cluster frontend listening (bidding)");

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
