pub mod api;
pub mod app;
pub mod config;
pub mod engine;
pub mod intercept;
pub mod proxy;
pub mod rln;
pub mod store;
pub mod sync;
pub mod webhook;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
