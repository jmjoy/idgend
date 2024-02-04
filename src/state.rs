use crate::args::Args;
use arc_swap::ArcSwap;
use etcd_client::{Client, LeaderKey};
use flume::Receiver;
use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

struct InnerState {
    args: Args,
    etcd_client: Client,
    id_rx: Receiver<u64>,
    is_master: AtomicBool,
    master_url: ArcSwap<String>,
    leader_key: ArcSwap<Option<LeaderKey>>,
}

#[derive(Clone)]
pub struct AppState {
    inner: Arc<InnerState>,
}

impl AppState {
    pub fn new(args: Args, etcd_client: Client, id_rx: Receiver<u64>) -> Self {
        Self {
            inner: Arc::new(InnerState {
                args,
                etcd_client,
                id_rx,
                is_master: Default::default(),
                master_url: Default::default(),
                leader_key: Default::default(),
            }),
        }
    }

    #[inline]
    pub fn id_rx(&self) -> &Receiver<u64> {
        &self.inner.id_rx
    }

    #[inline]
    pub fn is_master(&self) -> bool {
        self.inner.is_master.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_master(&self) {
        self.inner.is_master.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub fn master_url(&self) -> Arc<String> {
        self.inner.master_url.load_full()
    }

    #[inline]
    pub fn set_master_url(&self, url: String) {
        self.inner.master_url.store(Arc::new(url));
    }

    #[inline]
    pub fn etcd_client(&self) -> &Client {
        &self.inner.etcd_client
    }

    #[inline]
    pub fn args(&self) -> &Args {
        &self.inner.args
    }

    pub fn swap_leader_key(&self, leader_key: Option<LeaderKey>) -> Option<LeaderKey> {
        self.inner
            .leader_key
            .swap(Arc::new(leader_key))
            .deref()
            .clone()
    }
}
