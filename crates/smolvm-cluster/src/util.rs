//! Small shared helpers.

use iroh::endpoint::Builder;

/// Optionally pin iroh's UDP socket to a single interface via
/// `SMOLVM_CLUSTER_BIND_ADDR` (an IP, or IP:port). By default iroh binds
/// `0.0.0.0`, enumerating *every* interface — on a host with many bridge
/// interfaces (docker, libvirt) that pollutes address discovery and destabilizes
/// path selection. Pinning to the real NIC gives iroh a single, consistent
/// address to reason about. No-op when the env var is unset.
pub(crate) fn apply_bind(builder: Builder) -> anyhow::Result<Builder> {
    let Some(raw) = std::env::var("SMOLVM_CLUSTER_BIND_ADDR")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return Ok(builder);
    };
    let raw = raw.trim();
    // Accept a bare IP (append :0 for an ephemeral port) or a full IP:port.
    let addr = if raw.contains(':') {
        raw.to_string()
    } else {
        format!("{raw}:0")
    };
    builder
        .clear_ip_transports()
        .bind_addr(addr.as_str())
        .map_err(|e| anyhow::anyhow!("invalid SMOLVM_CLUSTER_BIND_ADDR {addr:?}: {e:?}"))
}

/// Milliseconds since the unix epoch — used to seed unique sandbox ids.
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
