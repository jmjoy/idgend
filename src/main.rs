#![warn(rust_2018_idioms, clippy::dbg_macro, clippy::print_stdout)]
#![doc = include_str!("../README.md")]

mod args;
mod elect;
mod etcd;
mod http;
mod id;
mod log;
mod rt;
mod state;

use crate::{args::init_args, log::init_logger};
use elect::{run_elect_master, run_observe_master};
use etcd::init_etcd_client;
use futures::future::try_join_all;
use http::run_http_server;
use id::run_id_gen_worker;
use rt::{create_tokio_runtime, join_handle, shutdown_signal};
use state::AppState;
use tokio::sync::oneshot;

fn main() -> anyhow::Result<()> {
    let args = init_args();

    init_logger(&args);

    let rt = create_tokio_runtime(&args)?;

    rt.block_on(async move {
        let etcd_client = init_etcd_client(&args).await?;

        let (id_tx, id_rx) = flume::bounded(0);

        let shutdown_signal = shutdown_signal();

        let app_state = AppState::new(args, etcd_client, id_rx);

        let (master_tx, master_rx) = oneshot::channel();

        try_join_all([
            join_handle(run_id_gen_worker(
                app_state.clone(),
                id_tx,
                master_rx,
                shutdown_signal.subscribe(),
            )),
            join_handle(run_http_server(
                app_state.clone(),
                shutdown_signal.subscribe(),
            )),
            join_handle(run_elect_master(
                app_state.clone(),
                master_tx,
                shutdown_signal.subscribe(),
            )),
            join_handle(run_observe_master(app_state, shutdown_signal.subscribe())),
        ])
        .await?;

        Ok(())
    })
}
