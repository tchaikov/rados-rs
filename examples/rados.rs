//! RADOS command-line tool
//!
//! A Rust implementation of the RADOS CLI for object operations.

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

#[derive(Parser)]
#[command(name = "rados")]
#[command(about = "RADOS object storage client", long_about = None)]
struct Cli {
    /// Pool name or ID
    #[arg(short, long)]
    pool: String,

    /// Ceph configuration file path
    #[arg(
        short = 'c',
        long,
        env = "CEPH_CONF",
        default_value = "/etc/ceph/ceph.conf"
    )]
    conf: String,

    /// Monitor addresses (comma-separated, e.g., "v2:127.0.0.1:3300")
    /// If not specified, will be read from ceph.conf
    #[arg(long, env = "MON_HOST")]
    mon_host: Option<String>,

    /// Keyring path
    /// If not specified, will be read from ceph.conf
    #[arg(long)]
    keyring: Option<String>,

    /// Entity name
    #[arg(long, default_value = "client.admin")]
    name: String,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Write object from file or stdin
    Put {
        /// Object name
        object: String,
        /// Input file ("-" for stdin)
        file: String,
    },
    /// Read object to file or stdout
    Get {
        /// Object name
        object: String,
        /// Output file ("-" for stdout)
        file: String,
    },
    /// Get object statistics
    Stat {
        /// Object name
        object: String,
    },
    /// Remove object
    Rm {
        /// Object name
        object: String,
    },
    /// List objects in pool
    Ls {
        /// Maximum number of objects to list (default: 100)
        #[arg(short, long, default_value = "100")]
        max: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(if cli.debug {
            tracing::Level::DEBUG
        } else {
            tracing::Level::WARN
        })
        .with_writer(std::io::stderr)
        .init();

    let ceph_config = if Path::new(&cli.conf).exists() {
        debug!("Loading configuration from: {}", cli.conf);
        Some(
            rados::cephconfig::CephConfig::from_file(&cli.conf)
                .context("Failed to parse ceph.conf")?,
        )
    } else {
        debug!("Configuration file not found: {}", cli.conf);
        None
    };

    let mon_addrs: Vec<String> = if let Some(mon_host) = cli.mon_host {
        mon_host.split(',').map(|s| s.trim().to_string()).collect()
    } else if let Some(ref config) = ceph_config {
        config.mon_addrs().unwrap_or_default()
    } else {
        Vec::new()
    };

    let dns_srv_name = ceph_config
        .as_ref()
        .map(|c| c.mon_dns_srv_name())
        .unwrap_or_else(|| "ceph-mon".to_string());

    info!("Connecting to monitors: {:?}", mon_addrs);

    let keyring_path = if let Some(keyring) = cli.keyring {
        keyring
    } else if let Some(ref config) = ceph_config {
        config.keyring().unwrap_or_else(|_| {
            debug!("Keyring not found in ceph.conf, using default");
            "/etc/ceph/keyring".to_string()
        })
    } else {
        "/etc/ceph/keyring".to_string()
    };

    debug!("Using keyring: {}", keyring_path);

    let (osdmap_tx, osdmap_rx) = rados::msgr2::map_channel::<rados::monclient::MOSDMap>(64);

    let auth = rados::monclient::AuthConfig::from_keyring(cli.name.clone(), &keyring_path)
        .context("Failed to create auth config")?;

    let mon_config = rados::monclient::MonClientConfig {
        mon_addrs,
        auth: Some(auth),
        dns_srv_name,
        ..Default::default()
    };

    let mon_client = rados::monclient::MonClient::new(mon_config, Some(osdmap_tx.clone()))
        .await
        .context("Failed to create MonClient")?;

    mon_client
        .init()
        .await
        .context("Failed to initialize MonClient")?;

    debug!("Connected to monitor");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let fsid = mon_client.get_fsid().await;

    let client_inc = current_client_inc()?;

    let osd_config = rados::OSDClientConfig {
        client_inc,
        ..Default::default()
    };

    let osd_client = rados::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        osdmap_tx,
        osdmap_rx,
    )
    .await
    .context("Failed to create OSDClient")?;

    debug!("OSD client created");

    osd_client
        .wait_for_latest_osdmap(tokio::time::Duration::from_secs(10))
        .await
        .context("Failed to receive latest OSDMap")?;

    debug!("Latest OSDMap received");

    let pool_id = parse_pool(&cli.pool, &osd_client).await?;

    debug!("Using pool ID: {}", pool_id);

    let ioctx = rados::IoCtx::new(Arc::clone(&osd_client), pool_id)
        .await
        .context("Failed to create IoCtx")?;

    debug!("IoCtx created for pool {}", pool_id);

    match cli.command {
        Commands::Put { object, file } => {
            let data = read_input(&file).context("Failed to read input")?;
            let data_len = data.len();
            let result = ioctx
                .write_full(&object, data)
                .await
                .context("Failed to write object")?;

            if cli.debug {
                eprintln!(
                    "Wrote {} bytes to {} (version: {})",
                    data_len, object, result.version
                );
            }
        }
        Commands::Get { object, file } => {
            let result = ioctx
                .read(&object, 0, u64::MAX)
                .await
                .context("Failed to read object")?;

            write_output(&file, &result.data).context("Failed to write output")?;

            if cli.debug {
                eprintln!(
                    "Read {} bytes from {} (version: {})",
                    result.data.len(),
                    object,
                    result.version
                );
            }
        }
        Commands::Stat { object } => {
            let stat = ioctx.stat(&object).await.context("Failed to stat object")?;

            println!("{} mtime {:?} size {}", object, stat.mtime, stat.size);
        }
        Commands::Rm { object } => {
            ioctx
                .remove(&object)
                .await
                .context("Failed to delete object")?;

            if cli.debug {
                eprintln!("Removed {object}");
            }
        }
        Commands::Ls { max } => {
            let (objects, _cursor) = ioctx
                .list_objects(None, max)
                .await
                .context("Failed to list objects")?;

            for obj in &objects {
                println!("{obj}");
            }

            if cli.debug {
                eprintln!("Listed {} objects", objects.len());
            }
        }
    }

    Ok(())
}

fn current_client_inc() -> Result<u32> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_secs();

    u32::try_from(seconds)
        .context("system clock is too far past UNIX_EPOCH for OSD client incarnation")
}

async fn parse_pool(pool: &str, osd_client: &Arc<rados::OSDClient>) -> Result<u64> {
    if let Ok(id) = pool.parse::<u64>() {
        return Ok(id);
    }

    let osdmap = osd_client
        .get_osdmap()
        .await
        .context("OSDMap not available")?;

    osdmap
        .pool_name
        .iter()
        .find_map(|(&id, name)| (name == pool).then_some(id))
        .ok_or_else(|| anyhow!("Pool '{pool}' not found"))
}

fn read_input(file: &str) -> Result<Bytes> {
    let data = if file == "-" {
        let mut buffer = Vec::new();
        io::stdin()
            .read_to_end(&mut buffer)
            .context("Failed to read from stdin")?;
        buffer
    } else {
        std::fs::read(file).with_context(|| format!("Failed to read file: {file}"))?
    };
    Ok(Bytes::from(data))
}

fn write_output(file: &str, data: &[u8]) -> Result<()> {
    if file == "-" {
        io::stdout()
            .write_all(data)
            .context("Failed to write to stdout")?;
        io::stdout().flush().context("Failed to flush stdout")?;
    } else {
        std::fs::write(file, data).with_context(|| format!("Failed to write file: {file}"))?;
    }
    Ok(())
}
