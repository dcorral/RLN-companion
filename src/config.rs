use std::net::IpAddr;
use std::path::Path;

use reqwest::Url;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("webhook url and secret must be set")]
    MissingWebhook,
    #[error("invalid webhook.url: {0}")]
    InvalidWebhookUrl(String),
    #[error("rln.base_url {0} is not private; set rln.allow_public_url to override")]
    PublicRlnUrl(String),
    #[error("invalid rln.base_url: {0}")]
    InvalidRlnUrl(String),
    #[error("service.listen_port must not be 0")]
    InvalidPort,
    #[error("{0} must not be 0")]
    ZeroInterval(&'static str),
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub service: Service,
    pub rln: Rln,
    pub engine: Engine,
    pub sync: Sync,
    pub webhook: Webhook,
    pub database: Database,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Service {
    pub listen_port: u16,
    pub auth_token: Option<String>,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            listen_port: 3101,
            auth_token: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Rln {
    pub base_url: String,
    pub token: Option<String>,
    pub request_timeout_secs: u64,
    pub proxy_timeout_secs: u64,
    pub proxy_max_body_mb: u64,
    pub allow_public_url: bool,
}

impl Default for Rln {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:3001".into(),
            token: None,
            request_timeout_secs: 120,
            proxy_timeout_secs: 600,
            proxy_max_body_mb: 64,
            allow_public_url: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Engine {
    pub refresh_interval_secs: u64,
    pub skip_sync: bool,
    pub reap_interval_secs: u64,
    pub reconcile_backoff_secs: u64,
    pub reconcile_max_wait_secs: u64,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            refresh_interval_secs: 10,
            skip_sync: false,
            reap_interval_secs: 300,
            reconcile_backoff_secs: 5,
            reconcile_max_wait_secs: 600,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Sync {
    pub full_interval_secs: u64,
    pub page_size: u64,
}

impl Default for Sync {
    fn default() -> Self {
        Self {
            full_interval_secs: 600,
            page_size: 100,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Webhook {
    pub url: String,
    pub secret: String,
    pub max_attempts: u32,
    pub backoff_base_secs: u64,
    pub backoff_cap_secs: u64,
    pub dispatch_interval_secs: u64,
}

impl Default for Webhook {
    fn default() -> Self {
        Self {
            url: String::new(),
            secret: String::new(),
            max_attempts: 10,
            backoff_base_secs: 2,
            backoff_cap_secs: 300,
            dispatch_interval_secs: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Database {
    pub path: String,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            path: "companion.sqlite".into(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Config, ConfigError> {
        match path {
            None => Ok(Config::default()),
            Some(p) => Ok(toml::from_str(&std::fs::read_to_string(p)?)?),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.webhook.url.is_empty() || self.webhook.secret.is_empty() {
            return Err(ConfigError::MissingWebhook);
        }
        let hook = Url::parse(&self.webhook.url)
            .map_err(|e| ConfigError::InvalidWebhookUrl(format!("{}: {e}", self.webhook.url)))?;
        if !matches!(hook.scheme(), "http" | "https") {
            return Err(ConfigError::InvalidWebhookUrl(self.webhook.url.clone()));
        }
        if self.service.listen_port == 0 {
            return Err(ConfigError::InvalidPort);
        }
        for (name, value) in [
            (
                "engine.refresh_interval_secs",
                self.engine.refresh_interval_secs,
            ),
            ("engine.reap_interval_secs", self.engine.reap_interval_secs),
            (
                "engine.reconcile_backoff_secs",
                self.engine.reconcile_backoff_secs,
            ),
            ("sync.full_interval_secs", self.sync.full_interval_secs),
            (
                "webhook.dispatch_interval_secs",
                self.webhook.dispatch_interval_secs,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::ZeroInterval(name));
            }
        }
        let url = Url::parse(&self.rln.base_url)
            .map_err(|e| ConfigError::InvalidRlnUrl(format!("{}: {e}", self.rln.base_url)))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ConfigError::InvalidRlnUrl(self.rln.base_url.clone()));
        }
        let host = url
            .host_str()
            .ok_or_else(|| ConfigError::InvalidRlnUrl(self.rln.base_url.clone()))?;
        if !self.rln.allow_public_url && !is_private_host(host) {
            return Err(ConfigError::PublicRlnUrl(self.rln.base_url.clone()));
        }
        Ok(())
    }
}

fn is_private_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private(),
        Ok(IpAddr::V6(ip)) => {
            ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|v4| v4.is_loopback() || v4.is_private())
        }
        Err(_) => host == "localhost" || !host.contains('.'),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn sample_config_matches_defaults() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("sample-config.toml");
        let cfg = Config::load(Some(&path)).unwrap();
        assert_eq!(format!("{cfg:?}"), format!("{:?}", Config::default()));
    }

    #[test]
    fn file_overrides_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[engine]\nrefresh_interval_secs = 3\n[webhook]\nurl = \"http://127.0.0.1:9000/hook\"\nsecret = \"s\"\n").unwrap();
        let cfg = Config::load(Some(&path)).unwrap();
        assert_eq!(cfg.engine.refresh_interval_secs, 3);
        assert_eq!(cfg.service.listen_port, 3101);
    }

    #[test]
    fn unknown_field_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[engine]\nnope = 1\n").unwrap();
        assert!(matches!(
            Config::load(Some(&path)),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_webhook_and_public_rln_url() {
        let mut cfg = Config::load(None).unwrap();
        assert!(matches!(cfg.validate(), Err(ConfigError::MissingWebhook)));
        cfg.webhook.url = "http://127.0.0.1:9000/hook".into();
        cfg.webhook.secret = "s".into();
        assert!(cfg.validate().is_ok());
        cfg.rln.base_url = "http://203.0.113.5:3001".into();
        assert!(matches!(cfg.validate(), Err(ConfigError::PublicRlnUrl(_))));
        cfg.rln.allow_public_url = true;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rln_url_host_and_scheme_table() {
        let mut cfg = Config::load(None).unwrap();
        cfg.webhook.url = "http://127.0.0.1:9000/hook".into();
        cfg.webhook.secret = "s".into();
        let cases = [
            ("http://[::1]:3001", "ok"),
            ("http://[fd00::1]:3001", "ok"),
            ("http://[fe80::1]:3001", "ok"),
            ("http://[::ffff:10.0.0.1]:3001", "ok"),
            ("http://10.0.0.1:3001", "ok"),
            ("http://172.16.0.1:3001", "ok"),
            ("http://172.32.0.1:3001", "public"),
            ("http://192.168.1.1:3001", "ok"),
            ("http://0.0.0.0:3001", "public"),
            ("https://localhost:3001", "ok"),
            ("http://rln:3001", "ok"),
            ("http://rln.example.com:3001", "public"),
            ("file:///x", "invalid"),
            ("ftp://localhost:3001", "invalid"),
        ];
        for (url, expected) in cases {
            cfg.rln.base_url = url.into();
            let res = cfg.validate();
            let got = match res {
                Ok(()) => "ok",
                Err(ConfigError::PublicRlnUrl(_)) => "public",
                Err(ConfigError::InvalidRlnUrl(_)) => "invalid",
                Err(e) => panic!("{url}: unexpected {e}"),
            };
            assert_eq!(got, expected, "{url}");
        }
    }

    #[test]
    fn validate_rejects_invalid_webhook_url() {
        let mut cfg = Config::load(None).unwrap();
        cfg.webhook.secret = "s".into();
        for url in ["not a url", "ftp://hooks.example.com/x", "file:///hook"] {
            cfg.webhook.url = url.into();
            assert!(
                matches!(cfg.validate(), Err(ConfigError::InvalidWebhookUrl(_))),
                "{url}"
            );
        }
        cfg.webhook.url = "https://hooks.example.com/x".into();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_port() {
        let mut cfg = Config::load(None).unwrap();
        cfg.webhook.url = "http://127.0.0.1:9000/hook".into();
        cfg.webhook.secret = "s".into();
        cfg.service.listen_port = 0;
        assert!(matches!(cfg.validate(), Err(ConfigError::InvalidPort)));
    }

    #[test]
    fn validate_rejects_zero_intervals() {
        let mut base = Config::load(None).unwrap();
        base.webhook.url = "http://127.0.0.1:9000/hook".into();
        base.webhook.secret = "s".into();
        assert!(base.validate().is_ok());
        let expect_zero = |cfg: &Config, field: &str| match cfg.validate() {
            Err(ConfigError::ZeroInterval(name)) => assert_eq!(name, field),
            other => panic!("{field}: unexpected {other:?}"),
        };
        let mut cfg = base.clone();
        cfg.engine.refresh_interval_secs = 0;
        expect_zero(&cfg, "engine.refresh_interval_secs");
        let mut cfg = base.clone();
        cfg.engine.reap_interval_secs = 0;
        expect_zero(&cfg, "engine.reap_interval_secs");
        let mut cfg = base.clone();
        cfg.engine.reconcile_backoff_secs = 0;
        expect_zero(&cfg, "engine.reconcile_backoff_secs");
        let mut cfg = base.clone();
        cfg.sync.full_interval_secs = 0;
        expect_zero(&cfg, "sync.full_interval_secs");
        let mut cfg = base.clone();
        cfg.webhook.dispatch_interval_secs = 0;
        expect_zero(&cfg, "webhook.dispatch_interval_secs");
    }
}
