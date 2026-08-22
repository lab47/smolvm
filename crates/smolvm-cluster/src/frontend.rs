//! Frontend cluster agent — a [`ClusterNode`] in the frontend role, plus the
//! public HTTP listener that routes over the backend connections it holds.
//!
//! The connection/membership machinery lives in [`crate::link`]; this module is
//! the request router: place creates by pushed capacity, route by-name to the
//! owning backend (fan-out probe on a cache miss), and merge lists.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::EndpointId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::backends::BackendTable;
use crate::capacity::BidSpec;
use crate::config::{FrontendConfig, NOMINAL_MEM_MB};
use crate::http::{self, ReqHead};
use crate::link::{ClusterNode, NodeKind};
use crate::membership::{MemberRole, Membership};
use crate::registry::SandboxRegistry;
use crate::splice::splice_with_preamble;
use crate::util::now_millis;
use crate::wire::membership_token;

const RESP_CAP: usize = 4 * 1024 * 1024;

/// Routing state for the HTTP listener.
struct FeState {
    backends: Arc<BackendTable>,
    sandboxes: SandboxRegistry,
    membership: Arc<Membership>,
    id_ctr: AtomicU64,
    /// Optional e2b control API key (`SMOLVM_CONTROL_API_KEY`), enforced on the
    /// `/sandboxes` paths the frontend terminates and sent on internal probes.
    control_api_key: Option<String>,
}

impl FeState {
    fn gen_sandbox_id(&self) -> String {
        let n = now_millis();
        let c = self.id_ctr.fetch_add(1, Ordering::Relaxed);
        let mut seed = Vec::with_capacity(16);
        seed.extend_from_slice(&n.to_le_bytes());
        seed.extend_from_slice(&c.to_le_bytes());
        let h = blake3::hash(&seed);
        let b = h.as_bytes();
        format!("vm-{:02x}{:02x}{:02x}{:02x}", b[0], b[1], b[2], b[3])
    }

    fn any_backend(&self) -> Option<(EndpointId, Connection)> {
        self.backends.all().into_iter().next()
    }
}

// ---- forwarding ----

async fn forward_buffered(conn: &Connection, request: &[u8]) -> anyhow::Result<(u16, Vec<u8>)> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(request).await?;
    let resp = tokio::time::timeout(Duration::from_secs(30), recv.read_to_end(RESP_CAP))
        .await
        .map_err(|_| anyhow::anyhow!("timed out reading response"))?
        .map_err(|e| anyhow::anyhow!("read response: {e}"))?;
    let _ = send.finish();
    let status =
        http::parse_status(&resp).ok_or_else(|| anyhow::anyhow!("no status line ({} bytes)", resp.len()))?;
    Ok((status, resp))
}

async fn tunnel(conn: Connection, client: TcpStream, preamble: Vec<u8>) {
    match conn.open_bi().await {
        Ok((send, recv)) => splice_with_preamble(client, send, recv, preamble).await,
        Err(e) => tracing::warn!(error = %e, "frontend: cannot open stream to backend"),
    }
}

fn build_get(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nhost: cluster\r\naccept: application/json\r\nconnection: close\r\n\r\n")
        .into_bytes()
}

fn response_body(resp: &[u8]) -> &[u8] {
    match resp.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(p) => &resp[p + 4..],
        None => &[],
    }
}

async fn fanout(fe: &Arc<FeState>, request: Vec<u8>) -> Vec<(EndpointId, u16, Vec<u8>)> {
    let mut set: JoinSet<Option<(EndpointId, u16, Vec<u8>)>> = JoinSet::new();
    for (id, conn) in fe.backends.all() {
        let req = request.clone();
        set.spawn(async move {
            let (status, resp) = forward_buffered(&conn, &req).await.ok()?;
            Some((id, status, resp))
        });
    }
    let mut out = Vec::new();
    while let Some(res) = set.join_next().await {
        if let Ok(Some(t)) = res {
            out.push(t);
        }
    }
    out
}

async fn fanout_find_owner(fe: &Arc<FeState>, name: &str) -> Option<(EndpointId, Connection)> {
    let answers = fanout(fe, build_get(&format!("/api/v1/machines/{name}"))).await;
    let (id, _, _) = answers.into_iter().find(|(_, status, _)| *status == 200)?;
    fe.backends.forward_conn(&id).map(|c| (id, c))
}

async fn respond_json(client: &mut TcpStream, status: &str, body: String) {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    let _ = client.write_all(resp.as_bytes()).await;
    let _ = client.shutdown().await;
}

fn backends_json(fe: &FeState) -> String {
    let items: Vec<serde_json::Value> = fe
        .backends
        .snapshot()
        .into_iter()
        .map(|(id, direct, cap, age_ms)| {
            serde_json::json!({"id": id.to_string(), "direct_path": direct, "age_ms": age_ms, "capacity": cap})
        })
        .collect();
    serde_json::json!({ "count": items.len(), "backends": items }).to_string()
}

fn members_json(fe: &FeState) -> String {
    let items: Vec<serde_json::Value> = fe
        .membership
        .snapshot()
        .into_iter()
        .map(|(id, role, connected, age_ms)| {
            let role = match role {
                MemberRole::Frontend => "frontend",
                MemberRole::Backend => "backend",
            };
            serde_json::json!({"id": id.to_string(), "role": role, "connected": connected, "age_ms": age_ms})
        })
        .collect();
    serde_json::json!({ "count": items.len(), "members": items }).to_string()
}

fn sandboxes_json(fe: &FeState) -> String {
    let items: Vec<serde_json::Value> = fe
        .sandboxes
        .snapshot()
        .into_iter()
        .map(|(sandbox, backend)| serde_json::json!({ "sandbox": sandbox, "backend": backend.to_string() }))
        .collect();
    serde_json::json!({ "count": items.len(), "sandboxes": items }).to_string()
}

// ---- create / list / kill ----

struct SandboxRequest {
    id: String,
    body: Vec<u8>,
    spec: BidSpec,
    client_chosen: bool,
}

fn resolve_sandbox_request(body: &[u8], fe: &FeState) -> Option<SandboxRequest> {
    let mut val: serde_json::Value = serde_json::from_slice(body).ok()?;
    let obj = val.as_object_mut()?;
    let (id, client_chosen) = match obj.get("name").and_then(|v| v.as_str()) {
        Some(n) if !n.is_empty() => (n.to_string(), true),
        _ => {
            let id = fe.gen_sandbox_id();
            obj.insert("name".to_string(), serde_json::Value::String(id.clone()));
            (id, false)
        }
    };
    let mem_mb = obj.get("mem").and_then(|v| v.as_u64()).unwrap_or(NOMINAL_MEM_MB);
    let vcpus = obj.get("cpus").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    Some(SandboxRequest {
        id,
        body: serde_json::to_vec(&val).ok()?,
        spec: BidSpec { mem_mb, vcpus },
        client_chosen,
    })
}

async fn allocate_sandbox(fe: Arc<FeState>, mut client: TcpStream, head: ReqHead, mut buf: Vec<u8>, body_start: usize) {
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

    let Some(SandboxRequest { id: sandbox, body: create_body, spec, client_chosen }) =
        resolve_sandbox_request(&body, &fe)
    else {
        respond_json(&mut client, "400 Bad Request", serde_json::json!({"error": "cannot parse create body"}).to_string()).await;
        return;
    };

    if client_chosen {
        let taken = fe.sandboxes.contains(&sandbox) || fanout_find_owner(&fe, &sandbox).await.is_some();
        if taken {
            respond_json(&mut client, "409 Conflict", serde_json::json!({"error": format!("sandbox '{sandbox}' already exists")}).to_string()).await;
            return;
        }
    }

    let candidates = fe.backends.best_for(spec);
    tracing::info!(sandbox = %sandbox, candidates = candidates.len(), "cluster: placing sandbox");
    if candidates.is_empty() {
        respond_json(&mut client, "503 Service Unavailable", serde_json::json!({"error": "no backend can host this sandbox"}).to_string()).await;
        return;
    }

    let request = http::build_request(&head, &create_body);
    let mut last: Option<Vec<u8>> = None;
    for (id, conn, score) in candidates.into_iter().take(3) {
        match forward_buffered(&conn, &request).await {
            Ok((status, resp)) if status < 500 && status != 409 => {
                if status < 300 {
                    fe.sandboxes.place(&sandbox, id);
                    tracing::info!(sandbox = %sandbox, backend = %id.fmt_short(), score, "cluster: placed sandbox");
                }
                let _ = client.write_all(&resp).await;
                let _ = client.shutdown().await;
                return;
            }
            Ok((status, resp)) => {
                tracing::debug!(backend = %id.fmt_short(), status, "cluster: backend rejected create, trying next");
                last = Some(resp);
            }
            Err(e) => tracing::warn!(backend = %id.fmt_short(), error = %e, "cluster: forward exchange failed"),
        }
    }
    match last {
        Some(resp) => {
            let _ = client.write_all(&resp).await;
            let _ = client.shutdown().await;
        }
        None => respond_json(&mut client, "503 Service Unavailable", serde_json::json!({"error": "all backends failed to place sandbox"}).to_string()).await,
    }
}

async fn kill_sandbox(fe: Arc<FeState>, mut client: TcpStream, head: ReqHead, conn: Connection, sandbox: String) {
    let request = http::build_request(&head, &[]);
    match forward_buffered(&conn, &request).await {
        Ok((status, resp)) => {
            if status < 300 && fe.sandboxes.forget(&sandbox) {
                tracing::info!(sandbox = %sandbox, "cluster: sandbox killed, dropped from registry");
            }
            let _ = client.write_all(&resp).await;
            let _ = client.shutdown().await;
        }
        Err(e) => {
            tracing::warn!(error = %e, "kill: forward exchange failed");
            respond_json(&mut client, "502 Bad Gateway", serde_json::json!({"error": "sandbox owner did not respond"}).to_string()).await;
        }
    }
}

async fn list_merge(fe: Arc<FeState>, mut client: TcpStream) {
    let answers = fanout(&fe, build_get("/api/v1/machines")).await;
    let mut machines: Vec<serde_json::Value> = Vec::new();
    for (_, status, resp) in &answers {
        if *status != 200 {
            continue;
        }
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(response_body(resp)) {
            if let Some(arr) = v.get("machines").and_then(|m| m.as_array()) {
                machines.extend(arr.iter().cloned());
            }
        }
    }
    respond_json(&mut client, "200 OK", serde_json::json!({ "machines": machines }).to_string()).await;
}

// ---- e2b control surface (variants of the create/list helpers above) ----

/// `GET` with an `X-API-Key` header, for internal probes to auth-gated backends.
fn build_get_auth(path: &str, key: Option<&str>) -> Vec<u8> {
    let mut s = format!("GET {path} HTTP/1.1\r\nhost: cluster\r\naccept: application/json\r\nconnection: close\r\n");
    if let Some(k) = key {
        s.push_str("x-api-key: ");
        s.push_str(k);
        s.push_str("\r\n");
    }
    s.push_str("\r\n");
    s.into_bytes()
}

/// e2b `POST /sandboxes`: forward the e2b body unchanged (the backend resolves
/// the template and auto-starts), then record `sandboxID` → backend from the 2xx
/// response. Placement uses a nominal spec since e2b bodies carry no size hint.
async fn allocate_sandbox_e2b(fe: Arc<FeState>, mut client: TcpStream, head: ReqHead, mut buf: Vec<u8>, body_start: usize) {
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

    let candidates = fe.backends.best_for(BidSpec { mem_mb: NOMINAL_MEM_MB, vcpus: 1 });
    if candidates.is_empty() {
        respond_json(&mut client, "503 Service Unavailable", serde_json::json!({"code":503,"error_code":"unavailable","message":"no backend can host this sandbox"}).to_string()).await;
        return;
    }

    let request = http::build_request(&head, &body);
    let mut last: Option<Vec<u8>> = None;
    for (id, conn, score) in candidates.into_iter().take(3) {
        match forward_buffered(&conn, &request).await {
            Ok((status, resp)) if status < 500 && status != 409 => {
                if status < 300 {
                    if let Some(sid) = serde_json::from_slice::<serde_json::Value>(response_body(&resp))
                        .ok()
                        .and_then(|v| v.get("sandboxID").and_then(|s| s.as_str().map(String::from)))
                    {
                        fe.sandboxes.place(&sid, id);
                        tracing::info!(sandbox = %sid, backend = %id.fmt_short(), score, "cluster: placed sandbox (e2b)");
                    }
                }
                let _ = client.write_all(&resp).await;
                let _ = client.shutdown().await;
                return;
            }
            Ok((status, resp)) => {
                tracing::debug!(backend = %id.fmt_short(), status, "cluster: backend rejected e2b create, trying next");
                last = Some(resp);
            }
            Err(e) => tracing::warn!(backend = %id.fmt_short(), error = %e, "cluster: e2b forward exchange failed"),
        }
    }
    match last {
        Some(resp) => {
            let _ = client.write_all(&resp).await;
            let _ = client.shutdown().await;
        }
        None => respond_json(&mut client, "503 Service Unavailable", serde_json::json!({"code":503,"error_code":"unavailable","message":"all backends failed to place sandbox"}).to_string()).await,
    }
}

/// e2b `GET /v2/sandboxes`: fan out to each backend's own e2b list handler (with
/// the control key) and concatenate the arrays, recomputing running totals.
async fn list_merge_v2(fe: Arc<FeState>, mut client: TcpStream) {
    let answers = fanout(&fe, build_get_auth("/v2/sandboxes", fe.control_api_key.as_deref())).await;
    let mut merged: Vec<serde_json::Value> = Vec::new();
    for (_, status, resp) in &answers {
        if *status != 200 {
            continue;
        }
        if let Ok(serde_json::Value::Array(arr)) = serde_json::from_slice::<serde_json::Value>(response_body(resp)) {
            merged.extend(arr);
        }
    }
    let running = merged
        .iter()
        .filter(|v| v.get("state").and_then(|s| s.as_str()) == Some("running"))
        .count();
    let body = serde_json::Value::Array(merged).to_string();
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nX-Total-Running: {running}\r\nX-Next-Token: \r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    let _ = client.write_all(resp.as_bytes()).await;
    let _ = client.shutdown().await;
}

async fn serve_client(fe: Arc<FeState>, mut client: TcpStream) {
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

    if method == "GET" && route == "/health" {
        respond_json(&mut client, "200 OK", serde_json::json!({"status": "ok", "role": "frontend", "backends": fe.backends.len()}).to_string()).await;
        return;
    }
    if method == "GET" && route == "/cluster/roster" {
        respond_json(&mut client, "200 OK", backends_json(&fe)).await;
        return;
    }
    if method == "GET" && route == "/cluster/members" {
        respond_json(&mut client, "200 OK", members_json(&fe)).await;
        return;
    }
    if method == "GET" && route == "/cluster/sandboxes" {
        respond_json(&mut client, "200 OK", sandboxes_json(&fe)).await;
        return;
    }

    // e2b-shaped control surface: auth (if configured), then place / route / merge.
    if route == "/sandboxes" || route == "/v2/sandboxes" || route.starts_with("/sandboxes/") {
        if let Some(expected) = fe.control_api_key.as_deref() {
            if head.header("x-api-key") != Some(expected) {
                respond_json(&mut client, "401 Unauthorized", serde_json::json!({"code":401,"error_code":"unauthorized","message":"missing or invalid X-API-Key"}).to_string()).await;
                return;
            }
        }
        if method == "POST" && route == "/sandboxes" {
            allocate_sandbox_e2b(fe, client, head, buf, body_start).await;
            return;
        }
        if method == "GET" && route == "/v2/sandboxes" {
            list_merge_v2(fe, client).await;
            return;
        }
        if let Some(rest) = route.strip_prefix("/sandboxes/") {
            let sandbox = rest.split('/').next().unwrap_or("").to_string();
            let target = match fe.sandboxes.locate(&sandbox).and_then(|id| fe.backends.forward_conn(&id).map(|c| (id, c))) {
                Some(t) => Some(t),
                None => match fanout_find_owner(&fe, &sandbox).await {
                    Some((id, conn)) => {
                        fe.sandboxes.place(&sandbox, id);
                        Some((id, conn))
                    }
                    None => None,
                },
            };
            let Some((_id, conn)) = target else {
                respond_json(&mut client, "404 Not Found", serde_json::json!({"code":404,"error_code":"not_found","message":format!("sandbox '{sandbox}' not found")}).to_string()).await;
                return;
            };
            if method == "DELETE" && !rest.contains('/') {
                kill_sandbox(fe, client, head, conn, sandbox).await;
            } else {
                tunnel(conn, client, buf).await;
            }
            return;
        }
        respond_json(&mut client, "404 Not Found", serde_json::json!({"code":404,"error_code":"not_found","message":"unknown sandbox path"}).to_string()).await;
        return;
    }

    if method == "POST" && route == "/api/v1/machines" {
        allocate_sandbox(fe, client, head, buf, body_start).await;
        return;
    }
    if method == "GET" && route == "/api/v1/machines" {
        list_merge(fe, client).await;
        return;
    }

    if let Some(rest) = route.strip_prefix("/api/v1/machines/") {
        let sandbox = rest.split('/').next().unwrap_or("").to_string();
        let target = match fe.sandboxes.locate(&sandbox).and_then(|id| fe.backends.forward_conn(&id).map(|c| (id, c))) {
            Some(t) => Some(t),
            None => match fanout_find_owner(&fe, &sandbox).await {
                Some((id, conn)) => {
                    fe.sandboxes.place(&sandbox, id);
                    Some((id, conn))
                }
                None => None,
            },
        };
        let Some((_id, conn)) = target else {
            respond_json(&mut client, "404 Not Found", serde_json::json!({"error": format!("sandbox '{sandbox}' not found")}).to_string()).await;
            return;
        };
        if method == "DELETE" && !rest.contains('/') {
            kill_sandbox(fe, client, head, conn, sandbox).await;
        } else {
            tunnel(conn, client, buf).await;
        }
        return;
    }

    let Some((_id, conn)) = fe.any_backend() else {
        respond_json(&mut client, "503 Service Unavailable", serde_json::json!({"error": "no backend available"}).to_string()).await;
        return;
    };
    tunnel(conn, client, buf).await;
}

/// Run the frontend until `shutdown` resolves.
pub async fn run(cfg: FrontendConfig, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
    let secret = crate::identity::load_or_generate(&cfg.key_path)?;
    let backends = Arc::new(BackendTable::new());
    let node = ClusterNode::build(
        secret,
        NodeKind::Frontend {
            backends: backends.clone(),
        },
        membership_token(&cfg.secret),
    )
    .await?;
    let seeds = crate::backend::parse_ids(&cfg.seeds)?;
    node.start(seeds);

    let fe = Arc::new(FeState {
        backends,
        sandboxes: SandboxRegistry::new(),
        membership: node.membership.clone(),
        id_ctr: AtomicU64::new(0),
        control_api_key: std::env::var("SMOLVM_CONTROL_API_KEY")
            .ok()
            .filter(|s| !s.is_empty()),
    });

    let listener = TcpListener::bind(&cfg.listen).await?;
    let id = node.endpoint_id();
    println!("cluster frontend endpoint id: {id}");
    tracing::info!(listen = %cfg.listen, endpoint_id = %id, "cluster frontend listening (dynamic membership)");

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

    node.shutdown().await;
    Ok(())
}
