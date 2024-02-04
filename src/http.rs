use crate::state::AppState;
use anyhow::bail;
use axum::{extract::State, http::StatusCode, routing::get, Router};
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle};
use tracing::{debug, error, info};
use url::Url;

async fn id(State(state): State<AppState>) -> (StatusCode, String) {
    match gen_id(state).await {
        Ok(id) => {
            debug!(?id, "request id success");
            (StatusCode::OK, id)
        }
        Err(err) => {
            error!(?err, "request id failed");
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        }
    }
}

async fn gen_id(state: AppState) -> anyhow::Result<String> {
    if state.is_master() {
        gen_id_by_worker(state).await
    } else {
        gen_id_by_proxy(state).await
    }
}

async fn gen_id_by_worker(state: AppState) -> anyhow::Result<String> {
    Ok(state.id_rx().recv_async().await?.to_string())
}

async fn gen_id_by_proxy(state: AppState) -> anyhow::Result<String> {
    let master_url = state.master_url();
    let mut url = master_url.as_str().parse::<Url>()?;
    url.set_path("/id");

    let response = reqwest::get(url).await?;
    let status = response.status();
    let content = response.text().await?;

    if !status.is_success() {
        bail!("{}", content);
    } else {
        Ok(content)
    }
}

pub fn run_http_server(
    app_state: AppState, mut shutdown_rx: broadcast::Receiver<()>,
) -> JoinHandle<anyhow::Result<()>> {
    let addr = app_state.args().web_addr.clone();

    let app = Router::new().route("/id", get(id)).with_state(app_state);

    info!(%addr, "running web server");

    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                if let Err(err) = shutdown_rx.recv().await {
                    error!(?err, "receive shutdown signal failed");
                }
            })
            .await?;

        Ok(())
    })
}
