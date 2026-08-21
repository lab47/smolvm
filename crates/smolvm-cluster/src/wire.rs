//! Control-plane messages, exchanged over one uni-stream per direction on every
//! cluster connection. The data plane (forwarded HTTP requests) is a raw byte
//! tunnel on separate bi-streams and needs no framing.

use iroh::EndpointId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::capacity::CapacitySnapshot;
use crate::membership::MemberRole;

/// Cap on a single control message.
const MSG_CAP: usize = 256 * 1024;

/// Messages on a control stream, in order: one `Hello`, then a repeating mix of
/// `Members` and (from backends) `Capacity`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Control {
    /// First message: the sender's role + proof it shares the cluster secret.
    Hello { role: MemberRole, token: String },
    /// The sender's announced membership view.
    Members(Vec<(EndpointId, MemberRole)>),
    /// A backend's current capacity (backend → frontend only).
    Capacity(CapacitySnapshot),
}

/// The membership token derived from the shared secret, sent over iroh's
/// encrypted, endpoint-authenticated channel.
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
