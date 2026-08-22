//! `smolvm cluster` — inspect a running cluster **frontend** over HTTP.
//!
//! Unlike the rest of the CLI (which reads local state directly), a cluster
//! spans machines, so these commands query a frontend's HTTP surface:
//! `/cluster/roster`, `/cluster/members`, `/cluster/sandboxes`, and the merged
//! `/api/v1/machines`. A cluster frontend always listens on TCP, so the target
//! must be an `http(s)://` URL.

use std::collections::BTreeMap;

use clap::{Args, Subcommand};
use serde::Deserialize;

use smolvm::error::Error;

/// Inspect a cluster frontend: backends, members, placements, and machines.
#[derive(Debug, Args)]
pub struct ClusterCmd {
    /// Defaults to `status` when omitted.
    #[command(subcommand)]
    cmd: Option<ClusterSub>,

    /// Frontend base URL (TCP). Defaults to `$SMOLVM_API_URL`, else
    /// `http://127.0.0.1:8080`.
    #[arg(long, global = true)]
    url: Option<String>,

    /// `X-API-Key` for an auth-gated frontend (defaults to `$SMOLVM_API_KEY`).
    #[arg(long, global = true)]
    api_key: Option<String>,

    /// Emit raw JSON instead of a formatted table.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum ClusterSub {
    /// Backends, the machines placed on each, and their state (the default view).
    Status,
    /// Connected backends and their spare capacity.
    Backends,
    /// All cluster members (frontends and backends).
    Members,
    /// Sandbox → backend placement recorded by the frontend.
    #[command(visible_alias = "sandboxes")]
    Placements,
    /// All machines across the cluster, with their state.
    Machines,
}

// ---- wire shapes (subset of each endpoint's JSON) ----

#[derive(Debug, Deserialize)]
struct Roster {
    #[serde(default)]
    backends: Vec<Backend>,
}

#[derive(Debug, Deserialize)]
struct Backend {
    id: String,
    #[serde(default)]
    direct_path: bool,
    #[serde(default)]
    age_ms: u64,
    #[serde(default)]
    capacity: Capacity,
}

#[derive(Debug, Default, Deserialize)]
struct Capacity {
    #[serde(default)]
    cpu_used: f64,
    #[serde(default)]
    mem_available_mb: u64,
    #[serde(default)]
    running_vms: u64,
    #[serde(default)]
    stalled: bool,
}

#[derive(Debug, Deserialize)]
struct Members {
    #[serde(default)]
    members: Vec<Member>,
}

#[derive(Debug, Deserialize)]
struct Member {
    id: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    connected: bool,
    #[serde(default)]
    age_ms: u64,
}

#[derive(Debug, Deserialize)]
struct Placements {
    #[serde(default)]
    sandboxes: Vec<Placement>,
}

#[derive(Debug, Deserialize)]
struct Placement {
    backend: String,
    sandbox: String,
}

#[derive(Debug, Deserialize)]
struct MachineList {
    #[serde(default)]
    machines: Vec<Machine>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Machine {
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    cpus: u32,
    #[serde(default)]
    memory_mb: u32,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl ClusterCmd {
    pub fn run(self) -> smolvm::Result<()> {
        let base = self.resolve_url()?;
        let key = self
            .api_key
            .clone()
            .or_else(|| std::env::var("SMOLVM_API_KEY").ok())
            .filter(|s| !s.is_empty());
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::config("cluster command", format!("start async runtime: {e}")))?;
        rt.block_on(self.dispatch(&base, key.as_deref()))
    }

    fn resolve_url(&self) -> smolvm::Result<String> {
        let raw = self
            .url
            .clone()
            .or_else(|| std::env::var("SMOLVM_API_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
        if !(raw.starts_with("http://") || raw.starts_with("https://")) {
            return Err(Error::config(
                "cluster command",
                format!(
                    "--url must be an http(s) URL for a cluster frontend (got {raw:?}); \
                     frontends listen on TCP, not a unix socket"
                ),
            ));
        }
        Ok(raw.trim_end_matches('/').to_string())
    }

    async fn dispatch(&self, base: &str, key: Option<&str>) -> smolvm::Result<()> {
        let client = reqwest::Client::new();
        match self.cmd.as_ref().unwrap_or(&ClusterSub::Status) {
            ClusterSub::Backends => {
                let v = get(&client, base, "/cluster/roster", key).await?;
                if self.json {
                    print_json(&v);
                } else {
                    render_backends(&from_value::<Roster>(v, "/cluster/roster")?.backends);
                }
            }
            ClusterSub::Members => {
                let v = get(&client, base, "/cluster/members", key).await?;
                if self.json {
                    print_json(&v);
                } else {
                    render_members(&from_value::<Members>(v, "/cluster/members")?.members);
                }
            }
            ClusterSub::Placements => {
                let v = get(&client, base, "/cluster/sandboxes", key).await?;
                if self.json {
                    print_json(&v);
                } else {
                    render_placements(&from_value::<Placements>(v, "/cluster/sandboxes")?.sandboxes);
                }
            }
            ClusterSub::Machines => {
                let v = get(&client, base, "/api/v1/machines", key).await?;
                if self.json {
                    print_json(&v);
                } else {
                    render_machines(&from_value::<MachineList>(v, "/api/v1/machines")?.machines);
                }
            }
            ClusterSub::Status => {
                let roster_v = get(&client, base, "/cluster/roster", key).await?;
                let place_v = get(&client, base, "/cluster/sandboxes", key).await?;
                let mach_v = get(&client, base, "/api/v1/machines", key).await?;
                if self.json {
                    print_json(&serde_json::json!({
                        "backends": roster_v,
                        "placements": place_v,
                        "machines": mach_v,
                    }));
                } else {
                    let backends = from_value::<Roster>(roster_v, "/cluster/roster")?.backends;
                    let placements =
                        from_value::<Placements>(place_v, "/cluster/sandboxes")?.sandboxes;
                    let machines = from_value::<MachineList>(mach_v, "/api/v1/machines")?.machines;
                    render_status(&backends, &placements, &machines);
                }
            }
        }
        Ok(())
    }
}

// ---- fetch ----

async fn get(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    key: Option<&str>,
) -> smolvm::Result<serde_json::Value> {
    let mut req = client.get(format!("{base}{path}"));
    if let Some(k) = key {
        req = req.header("x-api-key", k);
    }
    let resp = req.send().await.map_err(|e| {
        Error::config(
            "cluster command",
            format!("cannot reach {base}{path}: {e}"),
        )
    })?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| Error::config("cluster command", format!("read {path}: {e}")))?;
    if status == reqwest::StatusCode::NOT_FOUND && path.starts_with("/cluster/") {
        return Err(Error::config(
            "cluster command",
            format!(
                "{base} has no {path} — is it a cluster frontend? \
                 (a single-process or backend serve has no /cluster endpoints)"
            ),
        ));
    }
    if !status.is_success() {
        return Err(Error::config(
            "cluster command",
            format!("{path} -> HTTP {}: {}", status.as_u16(), body.trim()),
        ));
    }
    serde_json::from_str(&body).map_err(|e| {
        Error::config(
            "cluster command",
            format!("parse {path}: {e}"),
        )
    })
}

fn from_value<T: serde::de::DeserializeOwned>(
    v: serde_json::Value,
    path: &str,
) -> smolvm::Result<T> {
    serde_json::from_value(v)
        .map_err(|e| Error::config("cluster command", format!("parse {path}: {e}")))
}

fn print_json(v: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
    );
}

// ---- render ----

/// First 12 hex chars of an endpoint id, with an ellipsis when truncated.
fn short(id: &str) -> String {
    if id.len() > 12 {
        format!("{}…", &id[..12])
    } else {
        id.to_string()
    }
}

fn secs(age_ms: u64) -> String {
    format!("{}s", age_ms / 1000)
}

fn render_backends(backends: &[Backend]) {
    if backends.is_empty() {
        println!("no backends connected");
        return;
    }
    println!(
        "{:<13} {:>4} {:>6} {:>10} {:<7} {:>5}",
        "BACKEND", "VMS", "CPU", "MEM_FREE", "PATH", "AGE"
    );
    for b in backends {
        let path = if b.direct_path { "direct" } else { "relay" };
        let stalled = if b.capacity.stalled { " [STALLED]" } else { "" };
        println!(
            "{:<13} {:>4} {:>6.1} {:>8}MB {:<7} {:>5}{}",
            short(&b.id),
            b.capacity.running_vms,
            b.capacity.cpu_used,
            b.capacity.mem_available_mb,
            path,
            secs(b.age_ms),
            stalled,
        );
    }
}

fn render_members(members: &[Member]) {
    if members.is_empty() {
        println!("no members");
        return;
    }
    println!("{:<13} {:<9} {:<10} {:>5}", "MEMBER", "ROLE", "CONNECTED", "AGE");
    for m in members {
        println!(
            "{:<13} {:<9} {:<10} {:>5}",
            short(&m.id),
            m.role,
            m.connected,
            secs(m.age_ms),
        );
    }
}

fn render_placements(placements: &[Placement]) {
    if placements.is_empty() {
        println!("no sandboxes placed");
        return;
    }
    println!("{:<20} {:<13}", "SANDBOX", "BACKEND");
    for p in placements {
        println!("{:<20} {:<13}", p.sandbox, short(&p.backend));
    }
}

fn render_machines(machines: &[Machine]) {
    if machines.is_empty() {
        println!("no machines");
        return;
    }
    println!(
        "{:<20} {:<10} {:>4} {:>8} {:<}",
        "MACHINE", "STATE", "CPU", "MEM", "TEMPLATE"
    );
    for m in machines {
        println!(
            "{:<20} {:<10} {:>4} {:>6}MB {:<}",
            m.name,
            m.state,
            m.cpus,
            m.memory_mb,
            m.metadata
                .get("e2b.templateID")
                .map(String::as_str)
                .unwrap_or("-"),
        );
    }
}

fn render_status(backends: &[Backend], placements: &[Placement], machines: &[Machine]) {
    let by_name: BTreeMap<&str, &Machine> =
        machines.iter().map(|m| (m.name.as_str(), m)).collect();

    println!(
        "cluster — {} backend(s), {} machine(s)\n",
        backends.len(),
        machines.len()
    );

    if backends.is_empty() {
        println!("no backends connected");
    }
    for b in backends {
        let path = if b.direct_path { "direct" } else { "relay" };
        let stalled = if b.capacity.stalled { "  [STALLED]" } else { "" };
        println!(
            "● backend {}  vms={} cpu={:.1} mem_free={}MB {} age={}{}",
            short(&b.id),
            b.capacity.running_vms,
            b.capacity.cpu_used,
            b.capacity.mem_available_mb,
            path,
            secs(b.age_ms),
            stalled,
        );
        let mine: Vec<&Placement> = placements.iter().filter(|p| p.backend == b.id).collect();
        if mine.is_empty() {
            println!("    (no placed sandboxes in the frontend registry)");
        } else {
            for p in mine {
                match by_name.get(p.sandbox.as_str()) {
                    Some(m) => println!(
                        "    - {}  [{}]  {}cpu/{}MB  tmpl={}",
                        p.sandbox,
                        m.state,
                        m.cpus,
                        m.memory_mb,
                        m.metadata
                            .get("e2b.templateID")
                            .map(String::as_str)
                            .unwrap_or("-"),
                    ),
                    None => println!("    - {}  (placed here but not in the machine list)", p.sandbox),
                }
            }
        }
    }

    // Machines present in the merged list but not attributed to any backend by
    // the frontend registry (e.g. created directly on a backend).
    let placed: std::collections::BTreeSet<&str> =
        placements.iter().map(|p| p.sandbox.as_str()).collect();
    let orphans: Vec<&Machine> = machines
        .iter()
        .filter(|m| !placed.contains(m.name.as_str()))
        .collect();
    if !orphans.is_empty() {
        println!("\n⚠ machines not attributed to a backend (seen in the merged list, absent from the registry):");
        for m in orphans {
            println!("    - {}  [{}]", m.name, m.state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_truncates_long_ids_and_keeps_short_ones() {
        assert_eq!(
            short("0b0821abd4fb3c21548b50b4dea97c3cf"),
            "0b0821abd4fb…"
        );
        assert_eq!(short("short"), "short");
        assert_eq!(short("exactly12chr"), "exactly12chr"); // 12 chars, no ellipsis
    }

    #[test]
    fn secs_floors_milliseconds() {
        assert_eq!(secs(0), "0s");
        assert_eq!(secs(1999), "1s");
        assert_eq!(secs(60_000), "60s");
    }

    #[test]
    fn parses_the_roster_wire_shape() {
        let v = serde_json::json!({
            "count": 1,
            "backends": [{
                "id": "abc",
                "direct_path": true,
                "age_ms": 1500,
                "capacity": {"cpu_used": 0.5, "mem_available_mb": 100, "running_vms": 2, "stalled": false}
            }]
        });
        let r: Roster = serde_json::from_value(v).unwrap();
        assert_eq!(r.backends.len(), 1);
        assert_eq!(r.backends[0].capacity.running_vms, 2);
        assert!(r.backends[0].direct_path);
    }

    #[test]
    fn machine_json_is_camelcase() {
        let v = serde_json::json!({
            "name": "vm-1", "state": "running", "cpus": 4, "memoryMb": 8192,
            "metadata": {"e2b.templateID": "alpine"}
        });
        let m: Machine = serde_json::from_value(v).unwrap();
        assert_eq!(m.memory_mb, 8192);
        assert_eq!(m.metadata.get("e2b.templateID").map(String::as_str), Some("alpine"));
    }
}
