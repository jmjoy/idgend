use anyhow::{bail, Context};
use futures::future::try_join_all;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng, Rng};
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use std::{
    convert::Infallible,
    env,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    rc::Rc,
    sync::{
        mpsc::{self, Receiver, Sender, SyncSender},
        Arc,
    },
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    runtime,
    runtime::Runtime,
    sync::Mutex,
    task,
    task::{spawn, spawn_blocking},
    time::delay_for,
};

const NUM_RAND_PREFIXES: u16 = u16::max_value();

type AMReceiver<T> = Arc<Mutex<Receiver<T>>>;

/// 命令行参数
#[derive(Debug, StructOpt)]
struct Opt {
    /// rocksdb数据存放位置
    #[structopt(long, parse(from_os_str))]
    data_dir: PathBuf,

    /// ID范围，纯数字，格式`x,y`，然后 x <= ID < y
    #[structopt(long, parse(try_from_str = parse_range))]
    range: (u64, u64),

    #[structopt(long)]
    grpc_addr: Option<String>,

    #[structopt(long)]
    http_addr: Option<String>,

    #[structopt(long, short = "j")]
    core_threads: Option<usize>,
}

fn parse_range(src: &str) -> anyhow::Result<(u64, u64)> {
    let src = src.split(',').collect::<Vec<&str>>();
    if src.len() != 2 {
        bail!("correct range format: `x,y`, such as: 10000,20000");
    }

    let range = (src[0].parse()?, src[1].parse()?);
    if range.0 >= range.1 {
        bail!("in range `x,y`, `y` must great than `x`");
    }

    if range.0 < NUM_RAND_PREFIXES as u64 || range.1 - range.0 < NUM_RAND_PREFIXES as u64 {
        bail!(
            "in range `x,y`, `x` should great than {} and `y` - `x` should great than {}",
            NUM_RAND_PREFIXES,
            NUM_RAND_PREFIXES
        );
    }

    Ok(range)
}

struct IdDB {
    db: DB,
    size: u64,
}

impl IdDB {
    fn new(opt: &Opt) -> anyhow::Result<Self> {
        let exists = opt.data_dir.exists();
        let mut options = Options::default();
        options.create_if_missing(!exists);
        let db = DB::open(&options, opt.data_dir.clone())?;

        if !exists {
            log::info!(
                "Starting pre-generate ids [{},{})...",
                opt.range.0,
                opt.range.1
            );
            generate_and_store_ids(&opt, &db)?;
            log::info!("Ids [{},{}) has written!", opt.range.0, opt.range.1);
        } else {
            log::info!(
                "Ids [{},{}) has generated previous.",
                opt.range.0,
                opt.range.1
            );
        }

        let size = db_size(&db)?;
        if size == 0 {
            bail!("id has exhausted!");
        }

        log::info!("current db key size: {}", size);

        Ok(Self { db, size })
    }
}

fn init_logger() {
    if let Err(_) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "INFO");
    }
    env_logger::init();
}

fn build_runtime(opt: &Opt) -> anyhow::Result<Runtime> {
    let mut builder = runtime::Builder::new();
    builder.threaded_scheduler().enable_all();
    if let Some(core_threads) = opt.core_threads {
        builder.core_threads(core_threads);
    }
    Ok(builder.build()?)
}

fn run(opt: Opt) -> anyhow::Result<()> {
    let db = IdDB::new(&opt)?;

    match (&opt.grpc_addr, &opt.http_addr) {
        (None, None) => {
            log::warn!("Neither grpc-addr or http-addr is setted, skip and quit.");
            Ok(())
        }
        (grpc_addr, http_addr) => {
            let mut rt = build_runtime(&opt)?;

            rt.block_on(async move {
                let (sender, receiver) = mpsc::sync_channel(0);
                task::spawn_blocking(move || start_id_generator(sender, db));
                let receiver = Arc::new(Mutex::new(receiver));

                let mut services = Vec::with_capacity(2);
                if let Some(grpc_addr) = grpc_addr {}

                if let Some(http_addr) = http_addr {
                    let service = async move {
                        let http_addr = http_addr
                            .to_socket_addrs()?
                            .next()
                            .context("parse http addr failed")?;

                        let receiver = receiver.clone();

                        let make_svc = make_service_fn(move |_conn| {
                            let receiver = receiver.clone();

                            async move {
                                Ok::<_, anyhow::Error>(service_fn(move |req| {
                                    handle_http_request(req, receiver.clone())
                                }))
                            }
                        });

                        let server = Server::bind(&http_addr).serve(make_svc);

                        server.await?;

                        Ok::<_, anyhow::Error>(())
                    };
                    services.push(service);
                }
                try_join_all(services).await?;

                Ok::<_, anyhow::Error>(())
            })?;

            Ok(())
        }
    }
}

fn generate_and_store_ids(opt: &Opt, db: &DB) -> anyhow::Result<()> {
    let mut rng = thread_rng();
    let prefixes = (0..NUM_RAND_PREFIXES).collect::<Vec<_>>();

    let ranges = (opt.range.0..opt.range.1).into_iter().chunks(10000);
    let ranges = ranges.into_iter();
    for range in ranges {
        let mut batch = WriteBatch::default();
        for num in range {
            let prefix = prefixes[rng.gen_range(0, prefixes.len())];
            let prefix: [u8; 2] = prefix.to_le_bytes();
            let num: [u8; 8] = num.to_le_bytes();
            let mut buf = [0u8; 10];
            for i in 0..2 {
                buf[i] = prefix[i];
            }
            for i in 0..8 {
                buf[i + 2] = num[i];
            }
            batch.put(&buf, &[])?;
        }
        db.write(batch)?;
    }

    Ok(())
}

async fn handle_http_request(
    req: Request<Body>,
    receiver: AMReceiver<u64>,
) -> anyhow::Result<Response<Body>> {
    if req.uri().path() != "/id" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())?);
    }

    let receiver = receiver.lock().await;
    let id = match receiver.recv() {
        Ok(id) => id,
        Err(e) => {
            log::error!("recv failed: {:?}", e);
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())?);
        }
    };
    drop(receiver);

    Ok(Response::new(id.to_string().into()))
}

fn start_id_generator(sender: SyncSender<u64>, db: IdDB) {
    let sender = Rc::new(sender);
    for (key, _) in db.db.iterator(IteratorMode::Start) {
        let sender = sender.clone();
        let mut id = [0u8; 8];
        for i in 0..8 {
            unsafe {
                *id.get_unchecked_mut(i) = *key.get_unchecked(i + 2);
            }
        }
        if let Err(e) = db.db.delete(key) {
            log::error!("delete id failed {:?}", e);
            continue;
        }
        let id = u64::from_le_bytes(id);
        match sender.send(id) {
            Ok(..) => log::debug!("generate id: {}", id),
            Err(e) => log::error!("send id failed: {:?}", e),
        }
    }
}

fn db_size(db: &DB) -> anyhow::Result<u64> {
    db.property_int_value("rocksdb.estimate-num-keys")?
        .context("get property `rocksdb.estimate-num-keys` failed")
}

fn main() {
    init_logger();

    let opt = Opt::from_args();

    if let Err(e) = run(opt) {
        log::error!("{:?}", e);
    }
}
