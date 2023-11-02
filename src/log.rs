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
use tracing::metadata::LevelFilter;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub fn init_logger() {
    let max_level =
        if [LevelFilter::OFF, LevelFilter::WARN, LevelFilter::ERROR].contains(&ARGS.log_level) {
            ARGS.log_level
        } else {
            LevelFilter::INFO
        };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(max_level)
        .with_env_filter(EnvFilter::new(format!("idgend={}", ARGS.log_level)))
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
}
