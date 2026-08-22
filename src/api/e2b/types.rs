//! e2b-shaped request/response types for the control-plane adapter.
//!
//! e2b's wire casing is inconsistent (`sandboxID`, `templateID`, `memoryMB`,
//! `allow_internet_access`, `autoPause`), so casing is set per field rather than
//! with a blanket `rename_all`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

fn default_timeout() -> u64 {
    15
}

/// `POST /sandboxes` request. Unknown e2b fields are ignored.
#[derive(Debug, Deserialize)]
pub struct CreateSandboxRequest {
    #[serde(rename = "templateID", default)]
    pub template_id: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(rename = "envVars", default)]
    pub env_vars: BTreeMap<String, String>,
    #[serde(rename = "autoPause", default)]
    pub auto_pause: Option<bool>,
    #[serde(rename = "allow_internet_access", default)]
    pub allow_internet_access: Option<bool>,

    // smolvm extensions: not part of the e2b wire shape. A stock e2b client never
    // sends these (its sizing comes from the template); the `@smolvm/e2b` SDK does,
    // so its create knobs survive the move to the e2b `/sandboxes` surface.
    /// Client-chosen sandbox id (stock e2b always server-generates ids).
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub cpus: Option<u8>,
    #[serde(rename = "memoryMb", default)]
    pub memory_mb: Option<u32>,
    /// Guest port numbers to expose through the preview proxy (host side is
    /// auto-allocated). Powers `Sandbox.getHost(port)`.
    #[serde(default)]
    pub ports: Option<Vec<u16>>,
    /// Overrides the default `["sleep", "infinity"]` workload command.
    #[serde(default)]
    pub cmd: Option<Vec<String>>,
    #[serde(default)]
    pub workdir: Option<String>,
}

/// Response for create and resume (201).
#[derive(Debug, Serialize)]
pub struct SandboxCreateResponse {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(rename = "envdVersion")]
    pub envd_version: String,
    #[serde(rename = "envdAccessToken")]
    pub envd_access_token: String,
    #[serde(rename = "trafficAccessToken")]
    pub traffic_access_token: Option<String>,
    pub domain: Option<String>,
    pub alias: Option<String>,
}

/// One entry in `GET /v2/sandboxes` and the base of the detail object.
#[derive(Debug, Serialize)]
pub struct SandboxListItem {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(rename = "startedAt")]
    pub started_at: Option<String>,
    #[serde(rename = "endAt")]
    pub end_at: Option<String>,
    #[serde(rename = "cpuCount")]
    pub cpu_count: u32,
    #[serde(rename = "memoryMB")]
    pub memory_mb: u32,
    #[serde(rename = "diskSizeMB")]
    pub disk_size_mb: u64,
    pub metadata: BTreeMap<String, String>,
    pub state: String,
    #[serde(rename = "envdVersion")]
    pub envd_version: String,
    pub alias: Option<String>,
}

/// `GET /sandboxes/{id}` detail: the list item plus lifecycle.
#[derive(Debug, Serialize)]
pub struct SandboxDetail {
    #[serde(flatten)]
    pub base: SandboxListItem,
    pub lifecycle: Lifecycle,
}

#[derive(Debug, Serialize)]
pub struct Lifecycle {
    #[serde(rename = "autoResume")]
    pub auto_resume: bool,
    #[serde(rename = "onTimeout")]
    pub on_timeout: String,
}

#[derive(Debug, Deserialize)]
pub struct SetTimeoutBody {
    pub timeout: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct ResumeBody {
    #[serde(default)]
    pub timeout: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct PauseBody {
    #[serde(default)]
    pub memory: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RefreshBody {
    #[serde(default)]
    pub duration: Option<u64>,
}

/// e2b error envelope.
#[derive(Debug, Serialize)]
pub struct E2bError {
    pub code: u16,
    pub error_code: String,
    pub message: String,
}
