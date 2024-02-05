use clap::Parser;
use std::net::SocketAddr;
use url::Url;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Rust log filter, enabled when env `RUST_LOG` not set.
    #[arg(long, default_value = "info")]
    pub rust_log_filter: String,

    /// Etcd server list.
    #[arg(long, required = true)]
    pub etcd_server: Vec<String>,

    /// Etcd prefix path.
    #[arg(long, default_value = "/idgend")]
    pub etcd_prefix: String,

    /// The addr for http api.
    #[arg(long)]
    pub web_addr: SocketAddr,

    /// Advertise client url, used by slave to proxy.
    #[arg(long)]
    pub advertise_client_url: Url,

    /// Count of tokio worker threads.
    #[arg(long)]
    pub worker_threads_count: Option<usize>,

    /// Count of tokio max blocking threads.
    #[arg(long)]
    pub max_blocking_threads_count: Option<usize>,
}

#[inline]
pub fn init_args() -> Args {
    Args::parse()
}
