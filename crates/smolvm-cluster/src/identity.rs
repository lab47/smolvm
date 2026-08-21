//! Persistent iroh node identity.
//!
//! A cluster node needs a stable [`iroh::NodeId`] (an ed25519 public key) across
//! restarts so peers can reach it by id. We persist the 32-byte secret key to a
//! file (0600) under the node's credentials dir and load-or-generate on startup.

use std::path::Path;

use iroh::SecretKey;

/// Load the node secret key from `path`, or generate and persist a new one.
///
/// The file holds the raw 32-byte ed25519 secret. Created 0600.
pub fn load_or_generate(path: &Path) -> std::io::Result<SecretKey> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("cluster key at {} is not 32 bytes", path.display()),
                )
            })?;
            Ok(SecretKey::from_bytes(&arr))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let secret = SecretKey::generate();
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            write_private(path, &secret.to_bytes())?;
            Ok(secret)
        }
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    use std::io::Write;
    f.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_persists_and_reloads_to_same_node_id() {
        let dir = std::env::temp_dir().join(format!("smolvm-cluster-key-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("cluster.key");

        let a = load_or_generate(&path).unwrap();
        let b = load_or_generate(&path).unwrap();
        assert_eq!(a.public(), b.public(), "reload must yield the same NodeId");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
