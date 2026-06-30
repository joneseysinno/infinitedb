//! Server-side API and session abstractions.

/// Protocol request/response dispatcher.
pub mod api;
/// Authenticated session state and authorization checks.
pub mod session;
/// Length-prefixed request/response framing.
pub mod tcp;
/// Tokio TCP server runtime.
pub mod runtime;
/// Off-runtime request execution pool.
pub mod executor;
