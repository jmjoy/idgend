use crate::args::Args;
use futures::Future;
use tokio::{runtime::Runtime, select, signal, sync::broadcast, task::JoinHandle};
use tracing::{error, info};

pub fn create_tokio_runtime(args: &Args) -> anyhow::Result<Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();

    builder.enable_all();

    if let Some(val) = &args.worker_threads_count {
        builder.worker_threads(*val);
    }

    if let Some(val) = &args.max_blocking_threads_count {
        builder.max_blocking_threads(*val);
    }

    Ok(builder.build()?)
}

#[inline]
pub async fn join_handle(handle: JoinHandle<anyhow::Result<()>>) -> anyhow::Result<()> {
    handle.await?
}

pub fn shutdown_signal() -> broadcast::Sender<()> {
    let (tx, _) = broadcast::channel(1);

    let tx_ = tx.clone();

    #[cfg(unix)]
    let terminate = async move {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;

                if let Err(err) = tx.send(()) {
                    error!(?err, "send shutdown signal failed");
                }

                info!("receive shutdown signal");
            }
            Err(err) => {
                error!(?err, "failed to install signal handler");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = async move {
        let _ = tx;
        std::future::pending::<()>().await
    };

    tokio::spawn(async move {
        terminate.await;
    });

    tx_
}

pub async fn select_with_shutdown(
    mut shutdown_rx: broadcast::Receiver<()>, fut: impl Future<Output = anyhow::Result<()>>,
    shutdown_fut: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    select! {
        rs = shutdown_rx.recv() => {
            rs?;
            shutdown_fut.await;
        }
        rs = fut => {
            rs?;
        }
    }
    Ok(())
}
