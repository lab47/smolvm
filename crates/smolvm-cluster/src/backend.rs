//! Backend cluster agent — a [`ClusterNode`] in the backend role.
//!
//! It dials its seed frontends, discovers the rest of the cluster's frontends via
//! membership, and keeps a connection to each; over every one it pushes capacity
//! and serves forwarded requests by bridging them to its local serve socket.

use std::str::FromStr;
use std::sync::Arc;

use iroh::EndpointId;

use crate::capacity::CapacitySource;
use crate::config::BackendConfig;
use crate::link::{ClusterNode, NodeKind};
use crate::wire::membership_token;

pub struct BackendAgent {
    node: Arc<ClusterNode>,
}

impl BackendAgent {
    pub async fn spawn(cfg: BackendConfig, capacity: Arc<dyn CapacitySource>) -> anyhow::Result<Self> {
        let seeds = parse_ids(&cfg.seeds)?;
        anyhow::ensure!(
            !seeds.is_empty(),
            "a backend needs at least one --cluster-bootstrap frontend id to seed from"
        );
        let secret = crate::identity::load_or_generate(&cfg.key_path)?;
        let node = ClusterNode::build(
            secret,
            NodeKind::Backend {
                local: cfg.local_serve,
                capacity,
            },
            membership_token(&cfg.secret),
        )
        .await?;
        node.start(seeds);
        Ok(Self { node })
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.node.endpoint_id()
    }

    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }
}

pub(crate) fn parse_ids(raw: &[String]) -> anyhow::Result<Vec<EndpointId>> {
    raw.iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| EndpointId::from_str(s).map_err(|e| anyhow::anyhow!("invalid frontend id {s:?}: {e}")))
        .collect()
}
