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

use crate::etcd::ETCD_CLIENT;
use etcd_client::{LockClient, LockOptions};
use std::{mem::zeroed, str::from_utf8};
use tokio::{runtime::Handle, task};
use tracing::{debug, error};

pub struct LockGuard {
    lock_key: String,
    lock_client: LockClient,
}

impl LockGuard {
    pub async fn lock(key: &str, timeout: i64) -> anyhow::Result<Self> {
        let mut lock_client = ETCD_CLIENT.lock_client();
        let mut lease_client = ETCD_CLIENT.lease_client();

        let resp = lease_client.grant(timeout, None).await?;
        debug!(id = resp.id(), ttl = resp.ttl(), "grant lease for lock",);
        let lease_id = resp.id();

        let lock_options = LockOptions::new().with_lease(lease_id);
        let resp = lock_client.lock(key, Some(lock_options)).await?;
        let lock_key = String::from_utf8(resp.key().to_vec())?;
        debug!(lock_key, "lock response");

        Ok(Self {
            lock_key,
            lock_client,
        })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        task::block_in_place(move || {
            Handle::current().block_on(async move {
                if let Err(err) = self.lock_client.unlock(&*self.lock_key).await {
                    error!(lock_key = self.lock_key, ?err, "unlock failed");
                }
            })
        });
    }
}
