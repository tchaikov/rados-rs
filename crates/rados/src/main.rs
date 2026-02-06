//! RADOS command-line tool
//!
//! A Rust implementation of the RADOS CLI for object operations.

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;
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

    // Setup logging
    let log_level = if cli.debug {
        tracing::Level::DEBUG
    } else {
        tracing::Level::WARN
    };

    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_writer(std::io::stderr)
        .init();

    // Parse ceph.conf if it exists
    let ceph_config = if Path::new(&cli.conf).exists() {
        debug!("Loading configuration from: {}", cli.conf);
        Some(cephconfig::CephConfig::from_file(&cli.conf).context("Failed to parse ceph.conf")?)
    } else {
        debug!("Configuration file not found: {}", cli.conf);
        None
    };

    // Get monitor addresses (CLI arg > ceph.conf)
    let mon_addrs = if let Some(mon_host) = cli.mon_host {
        mon_host.split(',').map(|s| s.trim().to_string()).collect()
    } else if let Some(ref config) = ceph_config {
        config
            .mon_addrs()
            .context("Failed to get monitor addresses from ceph.conf")?
    } else {
        return Err(anyhow!(
            "Monitor address not specified. Use --mon-host or provide a valid ceph.conf"
        ));
    };

    info!("Connecting to monitors: {:?}", mon_addrs);

    // Get keyring path (CLI arg > ceph.conf > default)
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

    // Create shared MessageBus FIRST - both MonClient and OSDClient must use the same bus
    let message_bus = Arc::new(msgr2::MessageBus::new());

    // Create MonClient with shared MessageBus
    let mon_config = monclient::MonClientConfig {
        entity_name: cli.name.clone(),
        mon_addrs,
        keyring_path: keyring_path.clone(),
        ..Default::default()
    };

    let mon_client = Arc::new(
        monclient::MonClient::new(mon_config, Arc::clone(&message_bus))
            .await
            .context("Failed to create MonClient")?,
    );

    // Initialize connection
    mon_client
        .init()
        .await
        .context("Failed to initialize MonClient")?;

    // Register MonClient handlers on MessageBus
    mon_client
        .clone()
        .register_handlers()
        .await
        .context("Failed to register MonClient handlers")?;

    debug!("Connected to monitor");

    // Wait for MonMap to arrive (contains FSID)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get FSID
    let fsid = mon_client.get_fsid().await;

    // Create OSD client with unique client_inc for this invocation
    // Use current timestamp to ensure uniqueness across CLI invocations
    let client_inc = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    let osd_config = osdclient::OSDClientConfig {
        entity_name: cli.name.clone(),
        keyring_path: Some(keyring_path.clone()),
        client_inc,
        ..Default::default()
    };

    let osd_client = osdclient::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        Arc::clone(&message_bus),
    )
    .await
    .context("Failed to create OSDClient")?;

    // Register OSDClient handlers on MessageBus
    osd_client
        .clone()
        .register_handlers()
        .await
        .context("Failed to register OSDClient handlers")?;

    debug!("OSD client created");

    // NOW subscribe to OSDMap - both MonClient and OSDClient are ready
    mon_client
        .subscribe("osdmap", 0, 0)
        .await
        .context("Failed to subscribe to OSDMap")?;

    // Wait for OSDMap to arrive
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    debug!("OSDMap received");

    // Parse pool (try as ID first, then as name)
    let pool_id = parse_pool(&cli.pool, &osd_client).await?;

    debug!("Using pool ID: {}", pool_id);

    // Create IoCtx for the pool
    let ioctx = osdclient::IoCtx::new(Arc::clone(&osd_client), pool_id)
        .await
        .context("Failed to create IoCtx")?;

    debug!("IoCtx created for pool {}", pool_id);

    // Execute command
    match cli.command {
        Commands::Put { object, file } => {
            let data = read_input(&file).context("Failed to read input")?;
            let result = ioctx
                .write_full(&object, data.clone())
                .await
                .context("Failed to write object")?;

            if cli.debug {
                eprintln!(
                    "Wrote {} bytes to {} (version: {})",
                    data.len(),
                    object,
                    result.version
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
                eprintln!("Removed {}", object);
            }
        }
        Commands::Ls { max } => {
            let (objects, _cursor) = ioctx
                .list_objects(None, max)
                .await
                .context("Failed to list objects")?;

            // Print each object on its own line
            for obj in &objects {
                println!("{}", obj);
            }

            if cli.debug {
                eprintln!("Listed {} objects", objects.len());
            }
        }
    }

    Ok(())
}

/// Parse pool name or ID
async fn parse_pool(pool: &str, osd_client: &Arc<osdclient::OSDClient>) -> Result<u64> {
    // Try parsing as integer first
    if let Ok(id) = pool.parse::<u64>() {
        return Ok(id);
    }

    // Otherwise, look up pool by name in OSDMap
    let osdmap = osd_client
        .get_osdmap()
        .await
        .context("OSDMap not available")?;

    // Search for pool by name
    for (pool_id, pool_name) in &osdmap.pool_name {
        if pool_name == pool {
            return Ok(*pool_id);
        }
    }

    Err(anyhow!("Pool '{}' not found", pool))
}

/// Read input from file or stdin
fn read_input(file: &str) -> Result<Bytes> {
    let data = if file == "-" {
        // Read from stdin
        let mut buffer = Vec::new();
        io::stdin()
            .read_to_end(&mut buffer)
            .context("Failed to read from stdin")?;
        buffer
    } else {
        // Read from file
        std::fs::read(file).context(format!("Failed to read file: {}", file))?
    };

    Ok(Bytes::from(data))
}

/// Write output to file or stdout
fn write_output(file: &str, data: &[u8]) -> Result<()> {
    if file == "-" {
        // Write to stdout
        io::stdout()
            .write_all(data)
            .context("Failed to write to stdout")?;
        io::stdout().flush().context("Failed to flush stdout")?;
    } else {
        // Write to file
        std::fs::write(file, data).context(format!("Failed to write file: {}", file))?;
    }

    Ok(())
}
