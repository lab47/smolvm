//! Wire messages carried over the cluster connection's control stream.
//!
//! The data plane (forwarded HTTP requests) is a raw byte tunnel and needs no
//! framing. The control plane — the backend proving membership and pushing
//! capacity — is a sequence of length-framed JSON messages on one uni-stream the
//! backend opens after connecting.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::capacity::CapacitySnapshot;

/// Cap on a single control message.
const MSG_CAP: usize = 64 * 1024;

/// Control-stream messages, backend → frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Control {
    /// First message: proves the backend shares the cluster secret.
    Hello { token: String },
    /// Periodic capacity update used for placement.
    Capacity(CapacitySnapshot),
}

/// The membership token derived from the shared secret. Sent over iroh's
/// encrypted, endpoint-authenticated channel, so it's a bearer proof of "knows
/// the secret", not a replayable password in the clear.
pub fn membership_token(secret: &str) -> String {
    blake3::hash(secret.as_bytes()).to_hex().to_string()
}

/// Length-prefix (4-byte BE) + JSON.
pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    let body = serde_json::to_vec(msg).expect("control message serializes");
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Read one length-framed message from a stream.
pub async fn read_framed<R, T>(r: &mut R) -> anyhow::Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MSG_CAP {
        anyhow::bail!("control message too large: {n} bytes");
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}
