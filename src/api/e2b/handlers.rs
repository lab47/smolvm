//! e2b control-plane adapter handlers. Each reuses the existing
//! `handlers::machines::*` lifecycle functions and only reshapes the wire.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;

use super::types::*;
use crate::api::error::ApiError;
use crate::api::handlers::machines;
use crate::api::state::ApiState;
use crate::api::types::{
    CreateMachineRequest, DeleteQuery, MachineInfo, SetTimeoutRequest, StartMachineQuery,
};

/// Stub envd version reported to e2b clients (smolvm has no envd).
const ENVD_VERSION: &str = "0.0.0-smolvm";

// ---- error mapping ----

/// Wraps an `ApiError` so `?` in an adapter handler yields the e2b error shape.
pub struct E2bErrorResponse(ApiError);

impl From<ApiError> for E2bErrorResponse {
    fn from(e: ApiError) -> Self {
        Self(e)
    }
}

impl IntoResponse for E2bErrorResponse {
    fn into_response(self) -> Response {
        let (status, code, message) = map_api_error(self.0);
        e2b_err(status, code, message)
    }
}

fn map_api_error(e: ApiError) -> (StatusCode, &'static str, String) {
    match e {
        ApiError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, "unauthorized", m),
        ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, "forbidden", m),
        ApiError::NotFound(m) => (StatusCode::NOT_FOUND, "not_found", m),
        ApiError::Conflict(m) => (StatusCode::CONFLICT, "conflict", m),
        ApiError::PortConflict(m) => (StatusCode::CONFLICT, "port_in_use", m),
        ApiError::CloneIdentityRejuvenationFailed(m) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", m)
        }
        ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, "bad_request", m),
        ApiError::Timeout => (
            StatusCode::REQUEST_TIMEOUT,
            "timeout",
            "request timed out".to_string(),
        ),
        ApiError::Unavailable(m) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", m),
        ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", m),
    }
}

fn e2b_err(status: StatusCode, code: &str, message: String) -> Response {
    (
        status,
        Json(E2bError {
            code: status.as_u16(),
            error_code: code.to_string(),
            message,
        }),
    )
        .into_response()
}

// ---- auth middleware ----

/// Enforce `X-API-Key` when a control key is configured; otherwise pass through.
pub async fn require_api_key(
    State(state): State<Arc<ApiState>>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(expected) = state.control_api_key() {
        let provided = req
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok());
        if provided != Some(expected) {
            return e2b_err(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid X-API-Key".to_string(),
            );
        }
    }
    next.run(req).await
}

// ---- helpers ----

fn domain() -> Option<String> {
    std::env::var("SMOLVM_PREVIEW_DOMAIN")
        .ok()
        .filter(|s| !s.is_empty())
}

fn rfc3339(secs: u64) -> Option<String> {
    chrono::DateTime::from_timestamp(secs as i64, 0).map(|d| d.to_rfc3339())
}

/// e2b state is only running|paused; everything else maps to paused.
fn e2b_state(state: &str) -> String {
    if state == "running" { "running" } else { "paused" }.to_string()
}

/// Metadata with the reserved `e2b.*` keys removed.
fn public_metadata(mut m: BTreeMap<String, String>) -> BTreeMap<String, String> {
    m.retain(|k, _| !k.starts_with("e2b."));
    m
}

fn template_of(info: &MachineInfo) -> String {
    info.metadata
        .get("e2b.templateID")
        .cloned()
        .unwrap_or_default()
}

fn to_create_response(info: &MachineInfo) -> SandboxCreateResponse {
    let template_id = template_of(info);
    SandboxCreateResponse {
        sandbox_id: info.name.clone(),
        alias: (!template_id.is_empty()).then(|| template_id.clone()),
        template_id,
        envd_version: ENVD_VERSION.to_string(),
        envd_access_token: String::new(),
        traffic_access_token: None,
        domain: domain(),
    }
}

fn to_list_item(info: &MachineInfo) -> SandboxListItem {
    let template_id = template_of(info);
    SandboxListItem {
        sandbox_id: info.name.clone(),
        alias: (!template_id.is_empty()).then(|| template_id.clone()),
        template_id,
        started_at: rfc3339(info.created_at),
        end_at: None,
        cpu_count: info.cpus as u32,
        memory_mb: info.mem,
        disk_size_mb: info.storage_gb.unwrap_or(0) * 1024,
        metadata: public_metadata(info.metadata.clone()),
        state: e2b_state(&info.state),
        envd_version: ENVD_VERSION.to_string(),
    }
}

fn to_detail(info: &MachineInfo) -> SandboxDetail {
    SandboxDetail {
        base: to_list_item(info),
        lifecycle: Lifecycle {
            auto_resume: true,
            on_timeout: "pause".to_string(),
        },
    }
}

/// Resolve an e2b `templateID` to a smolvm image source: a known template alias
/// becomes `from` (its artifact path); anything else is treated as an OCI image;
/// absent defaults to `alpine`.
async fn resolve_source(state: &Arc<ApiState>, template: Option<&str>) -> (Option<String>, Option<String>) {
    match template {
        None => (Some("alpine".to_string()), None),
        Some(t) => match machines::get_template(State(state.clone()), Path(t.to_string())).await {
            Ok(Json(tinfo)) => (None, Some(tinfo.path)),
            Err(_) => (Some(t.to_string()), None),
        },
    }
}

// ---- handlers ----

/// `POST /sandboxes` — create and start.
pub async fn create_sandbox(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<CreateSandboxRequest>,
) -> Result<Response, E2bErrorResponse> {
    let template_echo = req.template_id.clone().unwrap_or_else(|| "alpine".to_string());
    let (image, from) = resolve_source(&state, req.template_id.as_deref()).await;

    let mut metadata = req.metadata.clone();
    metadata.insert("e2b.templateID".to_string(), template_echo);

    let env: Vec<_> = req
        .env_vars
        .iter()
        .map(|(k, v)| json!({"name": k, "value": v}))
        .collect();
    let on_idle = if req.auto_pause == Some(false) { "kill" } else { "pause" };
    let network = req.allow_internet_access.unwrap_or(true);
    let cmd = match &req.cmd {
        Some(c) if !c.is_empty() => c.clone(),
        _ => vec!["sleep".to_string(), "infinity".to_string()],
    };

    let mut create_body = json!({
        "image": image,
        "from": from,
        "network": network,
        "timeoutSecs": req.timeout,
        "onIdle": on_idle,
        "env": env,
        "metadata": metadata,
        "cmd": cmd,
    });
    // smolvm-extension sizing/ports (see CreateSandboxRequest); absent for stock e2b.
    if let Some(name) = &req.name {
        create_body["name"] = json!(name);
    }
    if let Some(cpus) = req.cpus {
        create_body["cpus"] = json!(cpus);
    }
    if let Some(mem) = req.memory_mb {
        create_body["memoryMb"] = json!(mem);
    }
    if let Some(workdir) = &req.workdir {
        create_body["workdir"] = json!(workdir);
    }
    if let Some(ports) = &req.ports {
        // host 0 → server auto-allocates a free host port for the preview proxy.
        create_body["ports"] = json!(ports
            .iter()
            .map(|g| json!({"host": 0, "guest": g}))
            .collect::<Vec<_>>());
    }
    let create_req: CreateMachineRequest = serde_json::from_value(create_body)
        .map_err(|e| ApiError::BadRequest(format!("build create request: {e}")))?;

    let Json(info) = machines::create_machine(State(state.clone()), Json(create_req)).await?;
    let name = info.name.clone();

    // e2b create also starts; roll back if the start fails.
    if let Err(e) = machines::start_machine(
        State(state.clone()),
        Path(name.clone()),
        Query(StartMachineQuery::default()),
        None,
    )
    .await
    {
        let _ = machines::delete_machine(
            State(state.clone()),
            Path(name.clone()),
            Query(DeleteQuery {
                force: true,
                cascade: false,
            }),
        )
        .await;
        return Err(e.into());
    }

    Ok((StatusCode::CREATED, Json(to_create_response(&info))).into_response())
}

#[derive(Debug, Deserialize, Default)]
pub struct ListQuery {
    #[serde(default)]
    metadata: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

/// `GET /v2/sandboxes` — list (array + X-Total-Running / X-Next-Token headers).
pub async fn list_sandboxes(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<ListQuery>,
) -> Result<Response, E2bErrorResponse> {
    let Json(list) = machines::list_machines(State(state.clone())).await?;
    let filter = parse_metadata_filter(q.metadata.as_deref());
    let want_state = q.state.as_deref();

    let mut items: Vec<SandboxListItem> = list
        .machines
        .iter()
        .filter(|m| metadata_matches(&m.metadata, &filter))
        .map(to_list_item)
        .filter(|it| want_state.map_or(true, |s| it.state == s))
        .collect();

    let total_running = items.iter().filter(|it| it.state == "running").count();
    if let Some(limit) = q.limit {
        items.truncate(limit);
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-total-running",
        HeaderValue::from_str(&total_running.to_string()).unwrap(),
    );
    headers.insert("x-next-token", HeaderValue::from_static(""));
    Ok((headers, Json(items)).into_response())
}

/// `GET /sandboxes/{id}` — detail.
pub async fn get_sandbox(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<Response, E2bErrorResponse> {
    let Json(info) = machines::get_machine(State(state), Path(id)).await?;
    Ok((StatusCode::OK, Json(to_detail(&info))).into_response())
}

/// `POST /sandboxes/{id}/pause` — 204.
pub async fn pause_sandbox(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    _body: Option<Json<PauseBody>>,
) -> Result<StatusCode, E2bErrorResponse> {
    let _ = machines::pause_machine(State(state), Path(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /sandboxes/{id}/resume` — 201 (create-shaped).
pub async fn resume_sandbox(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    body: Option<Json<ResumeBody>>,
) -> Result<Response, E2bErrorResponse> {
    let Json(info) = machines::resume_machine(State(state.clone()), Path(id.clone())).await?;
    if let Some(Json(ResumeBody { timeout: Some(secs) })) = body {
        let _ = machines::set_timeout_machine(
            State(state.clone()),
            Path(id.clone()),
            Json(SetTimeoutRequest { timeout_secs: secs }),
        )
        .await?;
    }
    Ok((StatusCode::CREATED, Json(to_create_response(&info))).into_response())
}

/// `DELETE /sandboxes/{id}` — 204.
pub async fn delete_sandbox(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, E2bErrorResponse> {
    let _ = machines::delete_machine(
        State(state),
        Path(id),
        Query(DeleteQuery {
            force: true,
            cascade: false,
        }),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /sandboxes/{id}/timeout` — 204.
pub async fn set_timeout(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(body): Json<SetTimeoutBody>,
) -> Result<StatusCode, E2bErrorResponse> {
    let _ = machines::set_timeout_machine(
        State(state),
        Path(id),
        Json(SetTimeoutRequest {
            timeout_secs: body.timeout,
        }),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /sandboxes/{id}/refreshes` — 204 (refresh the idle deadline).
pub async fn refresh_sandbox(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    body: Option<Json<RefreshBody>>,
) -> Result<StatusCode, E2bErrorResponse> {
    match body {
        Some(Json(RefreshBody { duration: Some(secs) })) => {
            let _ = machines::set_timeout_machine(
                State(state),
                Path(id),
                Json(SetTimeoutRequest { timeout_secs: secs }),
            )
            .await?;
        }
        _ => {
            machines::touch_machine(State(state), Path(id)).await;
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

// ---- metadata filter ----

fn parse_metadata_filter(raw: Option<&str>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Some(raw) = raw.filter(|s| !s.is_empty()) else {
        return out;
    };
    // e2b encodes the filter as a `key=value&key2=value2` string.
    for pair in raw.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn metadata_matches(m: &BTreeMap<String, String>, filter: &BTreeMap<String, String>) -> bool {
    filter.iter().all(|(k, v)| m.get(k) == Some(v))
}
