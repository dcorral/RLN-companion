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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn http_client(
    timeout_secs: u64,
    redirect: reqwest::redirect::Policy,
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(timeout_secs))
        .redirect(redirect)
        .build()
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
