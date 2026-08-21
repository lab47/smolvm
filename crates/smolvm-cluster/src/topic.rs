//! Cluster gossip topic derivation.

use iroh_gossip::proto::TopicId;

/// Derive the cluster's gossip [`TopicId`] from a shared secret. Only nodes that
/// share the same `SMOLVM_CLUSTER_SECRET` compute the same topic and thus form
/// one swarm.
pub fn topic_from_secret(secret: &str) -> TopicId {
    let hash = blake3::hash(secret.as_bytes());
    TopicId::from_bytes(*hash.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_secret_same_topic() {
        assert_eq!(topic_from_secret("hunter2"), topic_from_secret("hunter2"));
        assert_ne!(topic_from_secret("a"), topic_from_secret("b"));
    }
}
