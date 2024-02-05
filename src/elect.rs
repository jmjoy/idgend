use crate::{
    etcd::{retry_timeout, RetryTime},
    rt::select_with_shutdown,
    state::AppState,
};
use anyhow::bail;
use etcd_client::{LeaderKey, LeaseClient, ResignOptions};
use futures::future::ready;
use std::time::Duration;
use tokio::{
    sync::{broadcast, oneshot},
    task::JoinHandle,
    time::{self},
};
use tracing::{debug, error, info};

pub fn run_elect_master(
    state: AppState, master_tx: oneshot::Sender<()>, shutdown_rx: broadcast::Receiver<()>,
) -> JoinHandle<anyhow::Result<()>> {
    let state_ = state.clone();

    tokio::spawn(select_with_shutdown(
        shutdown_rx,
        async move {
            let (lease_id, keep_alive_handle) =
                keep_alive(state.etcd_client().lease_client()).await?;

            let leader_key = retry_timeout(
            || async {
                let elect_key = generate_elect_key(&state.args().etcd_prefix);

                let mut election_client = state.etcd_client().election_client();
                let resp = election_client
                    .campaign(elect_key.as_str(), state.args().advertise_client_url.to_string(), lease_id)
                    .await?;

                let leader = resp.leader();
                info!(name =? leader.map(LeaderKey::name_str), lease_id = leader.map(LeaderKey::lease), "elect succeed, I'm master!");

                Ok(leader.cloned())
            },
            RetryTime::Unlimited,
        )
        .await?;

            state.set_master();
            state.swap_leader_key(leader_key);
            if master_tx.send(()).is_err() {
                bail!("master tx send failed");
            }

            keep_alive_handle.await?
        },
        async move {
            if let Some(leader_key) = state_.swap_leader_key(None) {
                info!("start to resign master");
                let mut election_client = state_.etcd_client().election_client();
                if let Err(err) = election_client
                    .resign(Some(ResignOptions::new().with_leader(leader_key)))
                    .await
                {
                    error!(?err, "resign master failed");
                }
            }
        },
    ))
}

async fn keep_alive(
    mut lease_client: LeaseClient,
) -> anyhow::Result<(i64, JoinHandle<anyhow::Result<()>>)> {
    let resp = lease_client.grant(8, None).await?;
    let lease_id = resp.id();
    debug!(ttl = resp.ttl(), lease_id, "elect grant lease ttl");

    let handle = tokio::spawn(async move {
        let lease_client = lease_client;

        retry_timeout(
            || async {
                let (mut keeper, _) = lease_client.clone().keep_alive(lease_id).await?;
                debug!(lease_id, "elect lease keep alive start");

                let mut ticker = time::interval(Duration::from_secs(5));
                loop {
                    ticker.tick().await;
                    keeper.keep_alive().await?;
                }
            },
            RetryTime::Unlimited,
        )
        .await?;

        Ok(())
    });

    Ok((lease_id, handle))
}

pub fn run_observe_master(
    app_state: AppState, shutdown_rx: broadcast::Receiver<()>,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(select_with_shutdown(
        shutdown_rx,
        async move {
            let elect_key = generate_elect_key(&app_state.args().etcd_prefix);

            retry_timeout(
            || async {
                let mut stream = app_state
                    .etcd_client()
                    .election_client()
                    .observe(&*elect_key)
                    .await?;

                loop {
                    if let Some(resp) = stream.message().await? {
                        let Some(kv) = resp.kv() else {
                            continue;
                        };
                        debug!(key = ?kv.key_str(), value = ?kv.value_str(), "observe master key");

                        app_state.set_master_url(kv.value_str()?.to_string());
                    }
                }
            },
            RetryTime::Unlimited,
        )
        .await?;

            Ok(())
        },
        ready(()),
    ))
}

pub fn generate_elect_key(etcd_prefix: &str) -> String {
    format!("{}/election", etcd_prefix)
}
