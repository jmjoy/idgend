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
    args::{Args, ARGS},
    elect::spawn_elect_master,
    etcd::init_etcd_client,
    http::run_http_server,
    id::IdGenerator,
    shard::{Shard, ShardInitializer},
};
use tokio::{
    fs,
    runtime::{self, Runtime},
};
use tracing::{error, info};

pub const MAX_SHARD_INDEX: u16 = u16::MAX;

pub const DEFAULT_MIN_ID: u64 = 1000000000;

pub const DEFAULT_MAX_ID: u64 = u32::MAX as u64;

pub const DEFAULT_LOCK_TIMEOUT: i64 = 300;

pub const DEFAULT_REFILL_SIZE: usize = 1000;

pub const DEFAULT_FILL_START_SIZE: usize = 2000;

pub const DEFAULT_FILL_STOP_SIZE: usize = 10000;

pub fn run() {
    let runtime = match create_tokio_runtime() {
        Ok(runtime) => runtime,
        Err(err) => {
            error!(?err, "create runtime failed");
            return;
        }
    };
    if let Err(err) = runtime.block_on(run_app()) {
        error!(?err, "run failed");
    }
}

fn create_tokio_runtime() -> anyhow::Result<Runtime> {
    Ok(runtime::Builder::new_multi_thread().enable_all().build()?)
}

async fn run_app() -> anyhow::Result<()> {
    ensure_data_dir().await?;

    init_etcd_client();

    let elect_master_handle = spawn_elect_master();

    let mut shards = ShardInitializer::new();
    shards.start().await?;

    // loop {
    //     let index = Shard::retrieve().await?.index();
    //     dbg!(index);
    // }

    let mut id_generator = IdGenerator::new()?;
    id_generator.init().await?;

    run_http_server(id_generator).await?;

    Ok(())
}

async fn ensure_data_dir() -> anyhow::Result<()> {
    fs::create_dir_all(&ARGS.data_dir).await?;
    info!(
        data_dir = ARGS.data_dir.display().to_string(),
        "data directory is ready"
    );
    Ok(())
}
