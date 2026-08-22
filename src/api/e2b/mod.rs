//! e2b-shaped control-plane adapter: a thin `/sandboxes` surface that maps e2b
//! request/response shapes onto the existing `/api/v1/machines` lifecycle
//! handlers. Additive — the `/api/v1` surface is unchanged.

#[allow(missing_docs)]
pub mod handlers;
#[allow(missing_docs)]
pub mod types;
