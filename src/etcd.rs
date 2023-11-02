// Copyright 2023 jmjoy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::args::{Args, ARGS};
use etcd_client::{Client, ConnectOptions, Error::GRpcStatus, KvClient};
use futures::Future;
use once_cell::sync::Lazy;
use std::time::Duration;
use tokio::{runtime::Handle, sync::OnceCell, task};
use tonic::Code;
use tracing::{debug, info, instrument};

pub static ETCD_CLIENT: Lazy<Client> = Lazy::new(|| {
    task::block_in_place(move || {
        Handle::current().block_on(async move {
            let etcd_server = ARGS
                .etcd_server
                .iter()
                .map(|server| server.to_string())
                .collect::<Vec<_>>();

            let client = Client::connect(
                &etcd_server,
                Some(
                    ConnectOptions::new()
                        .with_timeout(Duration::from_secs(30))
                        .with_keep_alive(Duration::from_secs(10), Duration::from_secs(6)),
                ),
            )
            .await
            .expect("create etcd client failed");

            info!(endpoints = etcd_server.join(", "), "etcd client created");

            client
        })
    })
});

pub fn init_etcd_client() {
    Lazy::force(&ETCD_CLIENT);
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
                                return Err(err.into());
                            }
                        }
                        debug!("timeout, retry again");
                        continue;
                    }
                }
                return Err(err.into());
            }
        }
    }
}
