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

use crate::{
    args::ARGS,
    etcd::{retry_timeout, RetryTime, ETCD_CLIENT},
};
use etcd_client::{LeaderKey, ResignOptions};
use once_cell::sync::{Lazy, OnceCell};
use std::{
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    time::Duration,
};
use tokio::{
    signal, spawn,
    sync::oneshot,
    task::JoinHandle,
    time::{self, sleep},
};
use tonic::Code;
use tracing::{debug, error, info};

static IS_MASTER: AtomicBool = AtomicBool::new(false);

static ELECT_KEY: Lazy<String> = Lazy::new(|| format!("{}/election", ARGS.etcd_prefix));

pub fn spawn_elect_master() -> JoinHandle<anyhow::Result<()>> {
    let (lease_id_tx, lease_id_rx) = oneshot::channel();

    let keep_alive_handle: JoinHandle<Result<(), anyhow::Error>> = spawn(async move {
        let mut lease_client = ETCD_CLIENT.lease_client();
        let resp = lease_client.grant(8, None).await?;
        let lease_id = resp.id();
        debug!(ttl = resp.ttl(), lease_id, "elect grant lease ttl");
        lease_id_tx.send(lease_id).unwrap();

        let (mut keeper, _) = lease_client.keep_alive(lease_id).await?;
        debug!(lease_id, "elect lease keep alive start");
        let mut ticker = time::interval(Duration::from_secs(5));

        // keeper.keep_alive().await?;
        // if let Some(resp) = stream.message().await? {
        //     println!("lease {:?} keep alive, new ttl {:?}", resp.id(), resp.ttl());
        // }

        loop {
            ticker.tick().await;
            keeper.keep_alive().await?;
        }
    });

    spawn(async move {
        let lease_id = lease_id_rx.await.unwrap();

        retry_timeout(
            || async move {
                let mut election_client = ETCD_CLIENT.election_client();
                let resp = election_client
                    .campaign(ELECT_KEY.as_str(), "123", lease_id)
                    .await?;

                let leader = resp.leader().unwrap();
                info!(name =? leader.name_str(), lease_id = leader.lease(), "elect succeed, I'm master!");

                Ok(())
            },
            RetryTime::Unlimited,
        )
        .await?;

        IS_MASTER.store(true, Ordering::SeqCst);

        keep_alive_handle.await??;

        Ok(())
    })

    // let lease_id = loop {
    //     let resp = lease_client.grant(10, None).await?;
    //     let lease_id = resp.id();
    //     debug!("campaign grant lease ttl:{:?}, id:{:?}", resp.ttl(),
    // resp.id());

    //     match election_client.campaign(ELECT_KEY.as_str(), "123",
    // lease_id).await {         Ok(resp) => {
    //             let leader = resp.leader().unwrap().clone();
    //             info!(
    //                 "election name:{:?}, leaseId:{:?}",
    //                 leader.name_str(),
    //                 leader.lease()
    //             );

    //             spawn(async move {
    //                 signal::ctrl_c().await.unwrap();
    //                 println!("ctrl-c received!");
    //
    // election_client.resign(Some(ResignOptions::new().with_leader(leader.
    // clone()))).await.unwrap();                 std::process::exit(0);
    //             });

    //             break lease_id;
    //         },
    //         Err(err) => {
    //             if let etcd_client::Error::GRpcStatus(status) = &err {
    //                 if status.code() == Code::Cancelled {
    //                     continue;
    //                 }
    //             }
    //             return Err(err.into());
    //         }
    //     };
    // };

    // let (mut keeper, mut stream) = lease_client.keep_alive(lease_id).await?;
    // println!("lease {:?} keep alive start", lease_id);
    // loop {
    //     keeper.keep_alive().await?;
    //     sleep(Duration::from_secs(8)).await;
    // }

    // spawn(async move {
    //     loop {
    //         if let Ok(resp) =
    // ETCD_CLIENT.election_client().leader(ELECT_KEY.as_str()).await {
    //             dbg!(/*leader key*/resp.kv().map(|kv| kv.key_str()));
    //         } else {
    //             dbg!("no leader key");
    //         }
    //         sleep(Duration::from_secs(1)).await;
    //     }
    // });

    // sleep(Duration::from_secs(5)).await;

    // if let Some(resp) = stream.message().await? {
    //     println!("lease {:?} keep alive, new ttl {:?}", resp.id(),
    // resp.ttl()); }

    // loop {
    //     let resp = election_client.leader(ELECT_KEY.as_str()).await?;
    //     let kv = resp.kv().unwrap();
    //     println!("key is {:?}", kv.key_str());
    //     println!("value is {:?}", kv.value_str());

    //     sleep(Duration::from_secs(10)).await;
    // }
}

pub async fn observe_master() -> anyhow::Result<()> {
    retry_timeout(
        || async move {
            let mut stream = ETCD_CLIENT
                .election_client()
                .observe(ELECT_KEY.as_str())
                .await?;
            loop {
                if let Some(resp) = stream.message().await? {
                    debug!(key = ?resp.kv().unwrap().key_str(),  "observe master key");
                    break Ok(());
                }
            }
        },
        RetryTime::Unlimited,
    )
    .await?;

    Ok(())
}

pub fn is_master() -> bool {
    IS_MASTER.load(Ordering::SeqCst)
}
