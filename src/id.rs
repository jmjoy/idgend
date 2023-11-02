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
    app::{
        DEFAULT_FILL_START_SIZE, DEFAULT_FILL_STOP_SIZE, DEFAULT_MAX_ID, DEFAULT_MIN_ID,
        DEFAULT_REFILL_SIZE, MAX_SHARD_INDEX,
    },
    args::ARGS,
    shard::{self, Shard},
};
use flume::TrySendError;
use futures::future::select;
use once_cell::sync::Lazy;
use rand::{thread_rng, Rng};
use sled::{Db, Event, IVec};
use std::{
    io::Read,
    ops::RangeInclusive,
    path::PathBuf,
    sync::{atomic::AtomicU16, Arc},
};
use tokio::{
    fs, spawn,
    sync::{
        mpsc::{self},
        watch,
    },
    task::JoinHandle,
};
use tracing::{debug, error, info, warn};
use twox_hash::xxh3::hash64;

const LAST_CANDIDATE_ID_KEY: &[u8] =
    [Prefix::Shard as u8, ShardType::LastCandidateId as u8].as_slice();

const CURRENT_SHARD_INDEX_KEY: &[u8] =
    [Prefix::Shard as u8, ShardType::CurrentShardIndex as u8].as_slice();

static DB_PATH: Lazy<PathBuf> = Lazy::new(|| ARGS.data_dir.clone());

#[repr(u8)]
enum Prefix {
    Id = 0,
    Shard = 1,
}

#[repr(u8)]
enum ShardType {
    LastCandidateId = 0,
    CurrentShardIndex = 1,
}

#[derive(Clone)]
pub struct IdGenerator {
    db: Db,
    // refill_sender: mpsc::Sender<()>,
    // refill_handle: Arc<JoinHandle<()>>,
}

impl IdGenerator {
    pub fn new() -> anyhow::Result<Self> {
        let db = sled::open(&*DB_PATH)?;

        // let (refill_sender, refill_receiver) = mpsc::channel(1);
        // refill_sender.try_send(()).unwrap();

        // let refill_handle = Self::spawn_loop_refill(refill_receiver, db.clone());

        Ok(Self {
            db,
            // refill_sender,
            // refill_handle: Arc::new(refill_handle),
        })
    }

    pub async fn init(&mut self) -> anyhow::Result<()> {
        let mut db_size = self.db.scan_prefix([Prefix::Id as u8]).count();
        info!(db_size, "remained db size");

        let last_candidate_id = self
            .db
            .get(LAST_CANDIDATE_ID_KEY)?
            .map(|v| u64::from_le_bytes(<[u8; 8]>::try_from(v.as_ref()).unwrap()))
            .unwrap_or(DEFAULT_MIN_ID);

        let current_shard_index = match self.db.get(CURRENT_SHARD_INDEX_KEY)? {
            Some(index) => u64::from_le_bytes(<[u8; 8]>::try_from(index.as_ref()).unwrap()),
            None => {
                let shard = Shard::retrieve().await?;
                let index = shard.index();
                self.db.insert(
                    CURRENT_SHARD_INDEX_KEY,
                    index.to_le_bytes().as_slice(),
                )?;
                index
            }
        };

        let mut watcher = self.db.watch_prefix([Prefix::Id as u8]);

        let (refill_tx, refill_rx) = flume::bounded::<()>(0);
        let (fill_start_tx, fill_start_rx) = flume::bounded::<()>(0);
        let (fill_stop_tx, fill_stop_rx) = flume::bounded::<()>(0);

        spawn(async move {
            let db_size = &mut db_size;

            while let Some(event) = (&mut watcher).await {
                match event {
                    Event::Insert { key, value: _ } => {
                        debug!(id = key_to_id(&key), "insert id");
                        *db_size += 1;
                        if *db_size > DEFAULT_FILL_STOP_SIZE {
                            if let Err(TrySendError::Disconnected(_)) = fill_stop_tx.try_send(()) {
                                break;
                            }
                        }
                    }
                    Event::Remove { key } => {
                        debug!(id = key_to_id(&key), "remove id");
                        *db_size -= 1;
                        if *db_size < DEFAULT_REFILL_SIZE {
                            if let Err(TrySendError::Disconnected(_)) = refill_tx.try_send(()) {
                                break;
                            }
                        } else if *db_size < DEFAULT_FILL_START_SIZE {
                            if let Err(TrySendError::Disconnected(_)) = fill_start_tx.try_send(()) {
                                break;
                            }
                        }
                    }
                }
            }
        });

        spawn(async move {
            loop {
                refill_rx.recv_async().await;
            }
        });

        // if self.db.is_empty() {
        //     self.refill_sender.send(()).await?;
        // }

        Ok(())
    }

    pub async fn pop(&self) -> anyhow::Result<u64> {
        let (key, _) = self.db.pop_min()?.unwrap();
        Ok(key_to_id(&key))
    }

    fn spawn_loop_refill(mut refill_receiver: mpsc::Receiver<()>, mut db: Db) -> JoinHandle<()> {
        spawn(async move {
            loop {
                if refill_receiver.recv().await.is_none() {
                    info!("refill receive stopped");
                    break;
                };
                info!("start to refill");
                Self::refill(&mut db).await.unwrap();
            }
        })
    }

    async fn refill(db: &mut Db) -> anyhow::Result<()> {
        // let shard = Shard::retrieve().await?;
        // info!(index = shard.index(), "retrieve shard");

        // shard
        //     .collect_ids(|id| {
        //         let mut rng = thread_rng();
        //         let prefix = rng.gen_range(0..u8::MAX);
        //         let mut key = [0u8; 1 + 8];
        //         for (i, n) in prefix.to_le_bytes().into_iter().enumerate() {
        //             key[i] = n;
        //         }
        //         for (i, n) in id.to_le_bytes().into_iter().enumerate() {
        //             key[1 + i] = n;
        //         }
        //         db.insert(key, &[])?;
        //         Ok::<_, anyhow::Error>(())
        //     })
        //     .await?;

        // info!("refill finished");

        // let (min, max) = shard.calc_id_range();

        // let mut rng = thread_rng();
        // for id in min..=max {
        //     let key = rng.gen_range(0..DEFAULT_SHARD_COUNTS).to_le_bytes();
        //     self.db.insert(key, &id.to_le_bytes()[..])?;
        // }

        Ok(())
    }

    fn fill(db: &mut Db, last_candidate_id: u64, shard_index: u16) -> anyhow::Result<Option<u64>> {
        let mut rng = thread_rng();

        if last_candidate_id > DEFAULT_MAX_ID {
            return Ok(None);
        }

        for candidate_id in last_candidate_id..=DEFAULT_MAX_ID {
            let hash = hash64(candidate_id.to_le_bytes().as_slice());

            if hash >> (64 - 16) == shard_index as u64 {
                db.insert(
                    LAST_CANDIDATE_ID_KEY,
                    (candidate_id + 1).to_le_bytes().as_slice(),
                )?;

                let key = id_to_key(candidate_id);
                db.insert(key, &[])?;

                if candidate_id + 1 > DEFAULT_MAX_ID {
                    return Ok(None);
                }

                return Ok(Some(candidate_id + 1));
            }
        }

        Ok(None)
    }
}

fn key_to_id(key: &IVec) -> u64 {
    let key: &[u8] = &key.as_ref()[2..];
    let id = <[u8; 8]>::try_from(key).unwrap();
    u64::from_le_bytes(id)
}

fn id_to_key(id: u64) -> [u8; 9] {
    let salt = thread_rng().gen_range(0..u8::MAX);
    let mut key = [Prefix::Id as u8, salt, 0, 0, 0, 0, 0, 0, 0];
    (&mut key[2..]).copy_from_slice(id.to_le_bytes().as_slice());
    key
}

fn chunk_range(
    range: RangeInclusive<u64>, chunk_size: usize,
) -> impl Iterator<Item = RangeInclusive<u64>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + (chunk_size as u64) - 1).min(*range.end());
        block_start..=block_end
    })
}
