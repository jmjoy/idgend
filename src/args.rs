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

use anyhow::bail;
use clap::Parser;
use once_cell::sync::Lazy;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
};
use tracing::metadata::LevelFilter;

pub static ARGS: Lazy<Args> = Lazy::new(Args::parse);

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Log level (OFF, ERROR, WARN, INFO, DEBUG, TRACE).
    #[arg(long, default_value = "INFO")]
    pub log_level: LevelFilter,

    /// Etcd server list.
    #[arg(long, required = true)]
    pub etcd_server: Vec<SocketAddr>,

    /// Etcd prefix path.
    #[arg(long, default_value = "/idgend")]
    pub etcd_prefix: String,

    /// The addr for http api.
    #[arg(long, default_value = "0.0.0.0:12345")]
    pub http_addr: SocketAddr,

    /// The data storage directory.
    #[arg(long, default_value = "/var/lib/idgend")]
    pub data_dir: PathBuf,
}

pub fn init_args() {
    Lazy::force(&ARGS);
}
