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

use crate::{args::ARGS, etcd::ETCD_CLIENT, id::IdGenerator};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router, Server,
};
use etcd_client::{GetOptions, SortOrder, SortTarget};
use serde::Serialize;
use std::sync::Arc;
use tracing::info;
use tokio::{task::JoinHandle, spawn};

async fn id(State(id_gen): State<Arc<IdGenerator>>) -> (StatusCode, String) {
    let id = id_gen.pop().await.unwrap();
    (StatusCode::OK, id.to_string())
}

pub fn start_http_server(id_gen: IdGenerator) -> JoinHandle<anyhow::Result<()>> {
    spawn(run_http_server(id_gen))
}

async fn run_http_server(id_gen: IdGenerator) -> anyhow::Result<()> {
    let app = Router::new().route("/id", get(id).with_state(Arc::new(id_gen)));
    info!("listening on {}", &ARGS.http_addr);
    Server::bind(&ARGS.http_addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
