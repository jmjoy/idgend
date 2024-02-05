use crate::args::Args;
use etcd_client::{Client, ConnectOptions, Error::GRpcStatus};
use std::{future::Future, time::Duration};
use tonic::Code;
use tracing::{debug, info};

pub async fn init_etcd_client(args: &Args) -> anyhow::Result<Client> {
    let etcd_server = args
        .etcd_server
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    let client = Client::connect(
        &etcd_server,
        Some(
            ConnectOptions::new()
                .with_timeout(Duration::from_secs(30))
                .with_keep_alive(Duration::from_secs(10), Duration::from_secs(6)),
        ),
    )
    .await?;

    info!(endpoints = etcd_server.join(", "), "etcd client created");

    Ok(client)
}

#[derive(Debug, Clone, Copy)]
pub enum RetryTime {
    Unlimited,
    N(usize),
}

pub async fn retry_timeout<T, Fut>(
    f: impl Fn() -> Fut, retry_time: RetryTime,
) -> Result<T, etcd_client::Error>
where
    Fut: Future<Output = Result<T, etcd_client::Error>>,
{
    let mut time = 0;
    loop {
        match f().await {
            Ok(t) => break Ok(t),
            Err(err) => {
                if let GRpcStatus(status) = &err {
                    if status.code() == Code::Cancelled {
                        if let RetryTime::N(n) = retry_time {
                            time += 1;
                            if time >= n {
                                return Err(err);
                            }
                        }
                        debug!("timeout, retry again");
                        continue;
                    }
                }
                return Err(err);
            }
        }
    }
}
