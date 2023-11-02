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
    app::{DEFAULT_LOCK_TIMEOUT, DEFAULT_MAX_ID, DEFAULT_MIN_ID, MAX_SHARD_INDEX},
    args::ARGS,
    elect::{is_master, observe_master},
    etcd::{retry_timeout, RetryTime, ETCD_CLIENT},
    lock::LockGuard,
};
use anyhow::bail;
use etcd_client::{EventType, GetOptions, KvClient, SortOrder, SortTarget, WatchClient};
use futures::future::try_join_all;
use once_cell::sync::Lazy;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{
    ops::{Range, RangeInclusive},
    time::Duration,
};
use tokio::time::sleep;
use tracing::info;
use twox_hash::xxh3::hash64;

static SHARD_KEY_PREFIX: Lazy<String> = Lazy::new(|| format!("{}/shard/", ARGS.etcd_prefix));

static INITIALIZED_KEY: Lazy<String> =
    Lazy::new(|| format!("{}/shard-initialized", ARGS.etcd_prefix));

pub struct ShardInitializer {
    kv_client: KvClient,
}

impl ShardInitializer {
    pub fn new() -> Self {
        Self {
            kv_client: ETCD_CLIENT.kv_client(),
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        // Observe the master to be elected.
        observe_master().await?;

        // Check wether the shards have initialized.
        let is_initialized = self.check_initialized().await?;
        if is_initialized {
            info!("shards have initialized, ignored");
            return Ok(());
        }

        // Sleep to wait the IS_MASTER be set.
        sleep(Duration::from_secs(1)).await;

        if is_master() {
            // Master: initialize shards and set initialized.
            info!("shards are initializing");
            self.initialize_shards().await?;
            self.set_initialized().await?;
            info!("shards initialization finished");
        } else {
            // Slave: wait shard initialized.
            info!("watching for shards initializing");
            self.watch_initialized().await?;
            info!("watched shards initialized");
        }

        Ok(())
    }

    async fn check_initialized(&mut self) -> anyhow::Result<bool> {
        let resp = self.kv_client.get(INITIALIZED_KEY.as_str(), None).await?;
        Ok(!resp.kvs().is_empty())
    }

    async fn set_initialized(&mut self) -> anyhow::Result<()> {
        self.kv_client
            .put(INITIALIZED_KEY.as_str(), [], None)
            .await?;
        Ok(())
    }

    /// Watch for initialized key created event.
    async fn watch_initialized(&mut self) -> anyhow::Result<()> {
        retry_timeout(
            || async move {
                let mut watch_client = ETCD_CLIENT.watch_client();
                let (_, mut stream) = watch_client.watch(INITIALIZED_KEY.as_str(), None).await?;
                while let Some(resp) = stream.message().await? {
                    if resp
                        .events()
                        .iter()
                        .any(|ev| ev.event_type() == EventType::Put)
                    {
                        break;
                    }
                }
                Ok(())
            },
            RetryTime::Unlimited,
        )
        .await?;
        Ok(())
    }

    async fn initialize_shards(&mut self) -> anyhow::Result<()> {
        let mut rng = thread_rng();

        let mut arr = [0u16; MAX_SHARD_INDEX as usize + 1];
        for i in 0..=MAX_SHARD_INDEX {
            arr[i as usize] = i;
        }

        arr.shuffle(&mut rng);

        for index in arr {
            let key = SHARD_KEY_PREFIX.to_string() + &index.to_string();
            self.kv_client.put(key, [], None).await?;
        }

        Ok(())
    }
}

pub struct Shard {
    kv_client: KvClient,
    index: u16,
}

impl Shard {
    pub async fn retrieve() -> anyhow::Result<Self> {
        // TODO only retrieve by master.
        let mut kv_client = ETCD_CLIENT.kv_client();

        let options = GetOptions::new()
            .with_limit(1)
            .with_prefix()
            .with_sort(SortTarget::Create, SortOrder::Ascend);

        let shard = kv_client
            .get(SHARD_KEY_PREFIX.as_str(), Some(options))
            .await?;

        let Some(kv) = shard.kvs().get(0) else {
            bail!("shards exhausted");
        };

        kv_client.delete(kv.key(), None).await?;

        let index = kv
            .key_str()?
            .chars()
            .skip(SHARD_KEY_PREFIX.len())
            .collect::<String>();
        let index = index.parse::<u16>()?;

        Ok(Self { index, kv_client })
    }

    pub fn index(&self) -> u16 {
        self.index
    }

    pub async fn collect_ids<E>(&self, f: impl Fn(u64) -> Result<(), E>) -> Result<(), E> {
        let mut futs = Vec::with_capacity(3);
        for ids in chunk_range(
            DEFAULT_MIN_ID..=DEFAULT_MAX_ID,
            (DEFAULT_MAX_ID / 65535) as usize,
        ) {
            futs.push(async {
                for id in ids {
                    let hash = hash64(&id.to_le_bytes()[..]);
                    if hash >> (64 - 16) == self.index {
                        f(id)?;
                        sleep(Duration::from_millis(1000)).await;
                    }
                }
                Ok::<_, E>(())
            });
        }
        try_join_all(futs).await?;
        Ok(())
    }
}

fn chunk_range(
    range: RangeInclusive<u64>, chunk_size: usize,
) -> impl Iterator<Item = RangeInclusive<u64>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + (chunk_size as u64) - 1).min(*range.end());
        block_start..=block_end
    })
}
