use anyhow::{bail, Context};
use futures::future::{try_join_all, try_select, Either, FutureExt};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use itertools::Itertools;
use rand::{thread_rng, Rng};
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use std::{
    env,
    net::ToSocketAddrs,
    path::PathBuf,
    sync::{
        mpsc::{self, Receiver, Sender, SyncSender},
        Arc,
    },
};
use structopt::StructOpt;
use tokio::{runtime, runtime::Runtime, sync::Mutex, task};

use crate::proto::{id_gen_server::IdGenServer, IdGenLogic};

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
            Self::generate_and_store_ids(&opt, &db)?;
            log::info!("Ids [{},{}) has written!", opt.range.0, opt.range.1);
        } else {
            log::info!(
                "Ids [{},{}) has generated previous.",
                opt.range.0,
                opt.range.1
            );
        }

        let size = Self::db_size(&db)?;
        if size == 0 {
            bail!("id has exhausted!");
        }

        log::info!("current db key size: {}", size);

        Ok(Self { db, size })
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

    fn start_id_generator(mut self, id_gen_sender: SyncSender<u64>, shutdown_sender: Sender<()>) {
        for (key, _) in self.db.iterator(IteratorMode::Start) {
            self.size -= 1;
            if self.size < NUM_RAND_PREFIXES as u64 {
                log::warn!("id count is below {}!", NUM_RAND_PREFIXES);
            }

            let mut id = [0u8; 8];
            for i in 0..8 {
                unsafe {
                    *id.get_unchecked_mut(i) = *key.get_unchecked(i + 2);
                }
            }

            if let Err(e) = self.db.delete(key) {
                log::error!("delete id failed {:?}", e);
                continue;
            }

            let id = u64::from_le_bytes(id);
            match id_gen_sender.send(id) {
                Ok(..) => log::debug!("generate id: {}", id),
                Err(e) => {
                    log::error!("send id failed: {:?}", e);
                    break;
                }
            }
        }

        if let Err(e) = shutdown_sender.send(()) {
            log::error!("shutdown failed: {:?}", e);
        }
    }

    fn db_size(db: &DB) -> anyhow::Result<u64> {
        db.property_int_value("rocksdb.estimate-num-keys")?
            .context("get property `rocksdb.estimate-num-keys` failed")
    }
}

struct IdServer<'a> {
    opt: &'a Opt,
    iddb: IdDB,
    id_gen: (SyncSender<u64>, AMReceiver<u64>),
    shutdown: (Sender<()>, Receiver<()>),
}

impl<'a> IdServer<'a> {
    fn new(opt: &'a Opt, iddb: IdDB) -> anyhow::Result<Self> {
        let (id_gen_sender, id_gen_receiver) = mpsc::sync_channel(0);
        let id_gen = (id_gen_sender, Arc::new(Mutex::new(id_gen_receiver)));
        let shutdown = mpsc::channel();
        Ok(Self {
            opt,
            iddb,
            id_gen,
            shutdown,
        })
    }

    fn run(self) -> anyhow::Result<()> {
        match (&self.opt.grpc_addr, &self.opt.http_addr) {
            (None, None) => {
                bail!("Neither grpc-addr or http-addr is setted, skip and quit.");
            }
            _ => {
                let mut rt = build_runtime(&self.opt)?;
                rt.block_on(self.start_services())?;
                Ok(())
            }
        }
    }

    async fn start_services(self) -> anyhow::Result<()> {
        let iddb = self.iddb;
        let id_gen_sender = self.id_gen.0;
        let id_gen_receiver = self.id_gen.1;
        let shutdown_sender = self.shutdown.0;
        let shutdown_receiver = self.shutdown.1;

        task::spawn_blocking(move || iddb.start_id_generator(id_gen_sender, shutdown_sender));

        let mut services = Vec::with_capacity(2);

        if let Some(grpc_addr) = &self.opt.grpc_addr {
            services.push(Self::start_grpc_service(grpc_addr, id_gen_receiver.clone()).boxed());
        }

        if let Some(http_addr) = &self.opt.http_addr {
            services.push(Self::start_http_service(http_addr, id_gen_receiver.clone()).boxed());
        }

        let services_futs = try_join_all(services);
        let shutdown_fut = task::spawn_blocking(move || {
            if let Err(e) = shutdown_receiver.recv() {
                log::error!("shutdown recv failed: {:?}", e);
            }
        });
        match try_select(shutdown_fut, services_futs).await {
            Err(Either::Left((e, _))) => Err(e)?,
            Err(Either::Right((e, _))) => Err(e)?,
            _ => {}
        }

        Ok(())
    }

    async fn start_http_service(http_addr: &str, receiver: AMReceiver<u64>) -> anyhow::Result<()> {
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
        log::info!("http listen: {}", http_addr);
        server.await?;

        Ok(())
    }

    async fn start_grpc_service(grpc_addr: &str, receiver: AMReceiver<u64>) -> anyhow::Result<()> {
        let grpc_addr = grpc_addr
            .to_socket_addrs()?
            .next()
            .context("parse grpc addr failed")?;

        let id_gen_logic = IdGenLogic { receiver };
        log::info!("grpc listen: {}", grpc_addr);
        Ok(tonic::transport::Server::builder()
            .add_service(IdGenServer::new(id_gen_logic))
            .serve(grpc_addr)
            .await?)
    }
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

mod proto {
    use crate::AMReceiver;
    use tonic::{Request, Response, Status};

    tonic::include_proto!("idgen");

    pub(crate) struct IdGenLogic {
        pub(crate) receiver: AMReceiver<u64>,
    }

    #[tonic::async_trait]
    impl self::id_gen_server::IdGen for IdGenLogic {
        async fn id(&self, _request: Request<()>) -> Result<tonic::Response<u64>, Status> {
            let receiver = self.receiver.lock().await;
            let id = match receiver.recv() {
                Ok(id) => id,
                Err(e) => {
                    log::error!("recv failed: {:?}", e);
                    return Err(Status::internal("id gen failed"));
                }
            };
            drop(receiver);

            Ok(Response::new(id))
        }
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
    let iddb = IdDB::new(&opt)?;
    IdServer::new(&opt, iddb)?.run()?;
    Ok(())
}

fn main() {
    init_logger();

    let opt = Opt::from_args();

    if let Err(e) = run(opt) {
        log::error!("{:?}", e);
    }
}
