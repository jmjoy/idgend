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

#![warn(rust_2018_idioms, clippy::dbg_macro, clippy::print_stdout)]
#![doc = include_str!("../README.md")]

mod app;
mod args;
mod elect;
mod etcd;
mod http;
mod id;
mod lock;
mod log;
mod shard;
mod utils;

use crate::{
    app::run,
    args::{init_args, Args, ARGS},
    log::init_logger,
};
use clap::Parser;
use etcd::init_etcd_client;
use once_cell::sync::Lazy;

fn main() {
    init_args();
    init_logger();
    run();
}
