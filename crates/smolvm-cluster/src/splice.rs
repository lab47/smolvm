//! Bridge a duplex byte stream (TCP or Unix) with an iroh QUIC bi-stream.
//!
//! iroh bi-streams are a split `(SendStream, RecvStream)`, not one duplex, so we
//! can't use `copy_bidirectional` directly. We split the duplex and run two
//! copies, propagating half-close (FIN) in each direction — essential so a file
//! download / SSE stream ends cleanly.

use iroh::endpoint::{RecvStream, SendStream};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Splice a local duplex stream with an iroh bi-stream:
/// `duplex read → send` and `recv → duplex write`, until both directions close.
///
/// At the frontend the duplex is the client TCP socket; at the backend it's the
/// local serve socket. Same helper, both hops.
pub async fn splice<D>(duplex: D, send: SendStream, recv: RecvStream)
where
    D: AsyncRead + AsyncWrite + Send + 'static,
{
    splice_with_preamble(duplex, send, recv, Vec::new()).await
}

/// Like [`splice`], but first writes `preamble` to `send` — used to replay bytes
/// already read off the client socket (e.g. a peeked request line) into the
/// tunnel so the backend sees the complete, unmodified request.
pub async fn splice_with_preamble<D>(
    duplex: D,
    mut send: SendStream,
    mut recv: RecvStream,
    preamble: Vec<u8>,
) where
    D: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut dr, mut dw) = tokio::io::split(duplex);

    // duplex → send (client request bytes, or backend response bytes)
    let up = tokio::spawn(async move {
        if !preamble.is_empty() && send.write_all(&preamble).await.is_err() {
            let _ = send.finish();
            return;
        }
        let _ = tokio::io::copy(&mut dr, &mut send).await;
        // Signal EOF to the peer so the far side's `recv` copy completes.
        let _ = send.finish();
        let _ = send.stopped().await;
    });

    // recv → duplex (the other direction)
    let _ = tokio::io::copy(&mut recv, &mut dw).await;
    let _ = dw.shutdown().await;

    // The upstream copy ends when the local side closes; wait so we don't drop
    // `send` mid-flight.
    let _ = up.await;
}
