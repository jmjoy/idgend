use crate::{
    etcd::{retry_timeout, RetryTime},
    rt::select_with_shutdown,
    state::AppState,
};
use etcd_client::{GetOptions, KvClient};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::future::ready;
use tokio::{
    sync::{
        broadcast,
        oneshot::{self},
    },
    task::JoinHandle,
};
use tracing::info;

const FIRST_ID: u64 = 10_000_000_000_000_000_000;

const MAX_RAND_STEP: u64 = 1_000_000;

const RETRY_TIME: RetryTime = RetryTime::N(3);

pub fn run_id_gen_worker(
    app_state: AppState, id_tx: flume::Sender<u64>, master_rx: oneshot::Receiver<()>,
    shutdown_rx: broadcast::Receiver<()>,
) -> JoinHandle<anyhow::Result<()>> {
    let prefix = app_state.args().etcd_prefix.clone();

    tokio::spawn(select_with_shutdown(
        shutdown_rx,
        async move {
            master_rx.await?;

            info!("start id gen worker");

            let kv_client = app_state.etcd_client().kv_client();

            let last_id_key = format!("{}/last-id", prefix);

            let mut rng = SmallRng::from_entropy();

            loop {
                match id_gen(&kv_client, &last_id_key, &mut rng).await {
                    Ok(id) => id_tx.send_async(id).await?,
                    Err(err) => break Err(err),
                }
            }
        },
        ready(()),
    ))
}

async fn id_gen(
    kv_client: &KvClient, last_id_key: &str, rng: &mut impl Rng,
) -> anyhow::Result<u64> {
    let value = retry_timeout(
        || async {
            kv_client
                .clone()
                .get(last_id_key, Some(GetOptions::new().with_limit(1)))
                .await
        },
        RETRY_TIME,
    )
    .await?;

    let previous_id = match value.kvs().iter().next() {
        Some(kv) => {
            let value = kv.value();
            let value = <[u8; 8]>::try_from(value)?;
            u64::from_le_bytes(value)
        }
        None => FIRST_ID,
    };

    let step = rng.gen_range(0..MAX_RAND_STEP);

    let id = previous_id + step;

    retry_timeout(
        || async {
            kv_client
                .clone()
                .put(last_id_key, id.to_le_bytes().as_slice(), None)
                .await
        },
        RETRY_TIME,
    )
    .await?;

    Ok(id)
}
