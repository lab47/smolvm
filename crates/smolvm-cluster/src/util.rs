//! Small shared helpers.

use std::str::FromStr;

use iroh::EndpointId;

/// Parse gossip bootstrap ids (hex/z-base-32 endpoint ids), skipping blanks.
pub(crate) fn parse_ids(raw: &[String]) -> anyhow::Result<Vec<EndpointId>> {
    raw.iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            EndpointId::from_str(s)
                .map_err(|e| anyhow::anyhow!("invalid cluster endpoint id {s:?}: {e}"))
        })
        .collect()
}

/// Milliseconds since the unix epoch — used as a per-process announce epoch so a
/// restarted backend's announces supersede its old ones.
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
