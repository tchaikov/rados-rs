//! RADOS command-line tool
//!
//! A Rust implementation of the RADOS CLI for object operations.

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::io::{self, Read, Write};
use std::sync::Arc;
use tracing::{debug, info};

#[derive(Parser)]
#[command(name = "rados")]
#[command(about = "RADOS object storage client", long_about = None)]
struct Cli {
    /// Pool name or ID
    #[arg(short, long)]
    pool: String,

    /// Monitor addresses (comma-separated, e.g., "v2:127.0.0.1:3300")
    #[arg(long, env = "MON_HOST")]
    mon_host: Option<String>,

    /// Keyring path
    #[arg(long, default_value = "/etc/ceph/keyring")]
    keyring: String,

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

    // Get monitor address
    let mon_addrs = cli.mon_host.ok_or_else(|| {
        anyhow!("Monitor address not specified. Use --mon-host or set CEPH_MON_ADDR")
    })?;

    let mon_addrs: Vec<String> = mon_addrs.split(',').map(|s| s.trim().to_string()).collect();

    info!("Connecting to monitors: {:?}", mon_addrs);

    // Create MonClient
    let mon_config = monclient::MonClientConfig {
        entity_name: cli.name.clone(),
        mon_addrs,
        keyring_path: cli.keyring.clone(),
        ..Default::default()
    };

    let mon_client = Arc::new(
        monclient::MonClient::new(mon_config)
            .await
            .context("Failed to create MonClient")?,
    );

    // Initialize connection
    mon_client
        .init()
        .await
        .context("Failed to initialize MonClient")?;

    debug!("Connected to monitor");

    // Subscribe to OSDMap
    mon_client
        .subscribe("osdmap", 0, 0)
        .await
        .context("Failed to subscribe to OSDMap")?;

    // Wait for OSDMap to arrive
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    debug!("OSDMap received");

    // Create OSD client
    let osd_config = osdclient::OSDClientConfig {
        entity_name: cli.name.clone(),
        ..Default::default()
    };

    let osd_client = osdclient::OSDClient::new(osd_config, Arc::clone(&mon_client))
        .await
        .context("Failed to create OSDClient")?;

    debug!("OSD client created");

    // Parse pool (try as ID first, then as name)
    let pool_id = parse_pool(&cli.pool, &mon_client).await?;

    debug!("Using pool ID: {}", pool_id);

    // Execute command
    match cli.command {
        Commands::Put { object, file } => {
            let data = read_input(&file).context("Failed to read input")?;
            let result = osd_client
                .write_full(pool_id, &object, data.clone())
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
            let result = osd_client
                .read(pool_id, &object, 0, u64::MAX)
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
            let stat = osd_client
                .stat(pool_id, &object)
                .await
                .context("Failed to stat object")?;

            println!("{} mtime {:?} size {}", object, stat.mtime, stat.size);
        }
        Commands::Rm { object } => {
            osd_client
                .delete(pool_id, &object)
                .await
                .context("Failed to delete object")?;

            if cli.debug {
                eprintln!("Removed {}", object);
            }
        }
    }

    Ok(())
}

/// Parse pool name or ID
async fn parse_pool(pool: &str, mon_client: &Arc<monclient::MonClient>) -> Result<i64> {
    // Try parsing as integer first
    if let Ok(id) = pool.parse::<i64>() {
        return Ok(id);
    }

    // Otherwise, look up pool by name in OSDMap
    let osdmap = mon_client
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
