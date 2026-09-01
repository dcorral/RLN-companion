use std::path::PathBuf;
use std::process::ExitCode;

use bytes::Bytes;
use clap::Parser;
use rln_companion::app::{build_app, build_router, spawn_background, AppError};
use rln_companion::config::{Config, ConfigError};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rln-companion", version)]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    listen_port: Option<u16>,
    #[arg(long)]
    rln_url: Option<String>,
    #[arg(long)]
    db_path: Option<String>,
    /// RLN openapi.yaml to serve at /companion/openapi.yaml
    #[arg(long)]
    openapi: Option<PathBuf>,
}

fn apply_overrides(mut cfg: Config, args: &Args) -> Config {
    if let Some(port) = args.listen_port {
        cfg.service.listen_port = port;
    }
    if let Some(url) = &args.rln_url {
        cfg.rln.base_url = url.clone();
    }
    if let Some(path) = &args.db_path {
        cfg.database.path = path.clone();
    }
    cfg
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!(error = %e, "ctrl-c handler failed");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                error!(error = %e, "sigterm handler failed");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutting down");
}

async fn run(args: Args) -> Result<(), AppError> {
    let cfg = apply_overrides(Config::load(args.config.as_deref())?, &args);
    cfg.validate()?;
    let openapi = match &args.openapi {
        Some(p) => Some(Bytes::from(
            std::fs::read(p).map_err(|e| AppError::Openapi(p.clone(), e))?,
        )),
        None => None,
    };
    let app = build_app(&cfg, openapi).await?;
    app.engine.probe().await?;
    let port = cfg.service.listen_port;
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|e| AppError::Bind(port, e))?;
    let mut handles = spawn_background(&app);
    let engine = app.engine.clone();
    handles.push(tokio::spawn(async move {
        match engine.reconcile().await {
            Ok(true) => {}
            Ok(false) => warn!("reconcile gave up; serving with a stale mirror"),
            Err(e) => error!(error = %e, "reconcile failed"),
        }
    }));
    info!(port, "listening");
    let served = axum::serve(listener, build_router(app.state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await;
    for h in handles {
        h.abort();
    }
    app.state.store.close().await;
    served.map_err(AppError::Serve)
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "fatal");
            if matches!(e, AppError::Config(ConfigError::PublicRlnUrl(_))) {
                error!("rln-companion must be the only client of the node; keep the node's API port private or set rln.allow_public_url = true");
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn cli_overrides_win_over_loaded_config() {
        let mut cfg = Config::default();
        cfg.service.listen_port = 1;
        cfg.rln.base_url = "http://10.0.0.1:1".into();
        cfg.database.path = "file.sqlite".into();

        let args = Args::parse_from(["rln-companion"]);
        let same = apply_overrides(cfg.clone(), &args);
        assert_eq!(same.service.listen_port, 1);
        assert_eq!(same.rln.base_url, "http://10.0.0.1:1");
        assert_eq!(same.database.path, "file.sqlite");

        let args = Args::parse_from([
            "rln-companion",
            "--listen-port",
            "4000",
            "--rln-url",
            "http://10.0.0.2:3001",
            "--db-path",
            "other.sqlite",
        ]);
        let over = apply_overrides(cfg, &args);
        assert_eq!(over.service.listen_port, 4000);
        assert_eq!(over.rln.base_url, "http://10.0.0.2:3001");
        assert_eq!(over.database.path, "other.sqlite");
    }
}
