//! Dencoder: Tool for validating Ceph type encodings against corpus files
//! 
//! This tool reads Ceph corpus files and validates that our encoding/decoding
//! implementations match the official Ceph implementation.

mod corpus;
mod registry;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dencoder")]
#[command(about = "Validate Ceph type encodings against corpus files", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import and validate a type from a corpus file
    Import {
        /// Type name (e.g., "entity_addr_t", "utime_t")
        #[arg(short, long)]
        type_name: String,
        
        /// Path to corpus file
        #[arg(short, long)]
        corpus_file: PathBuf,
    },
    
    /// Export a type to a corpus file
    Export {
        /// Type name
        #[arg(short, long)]
        type_name: String,
        
        /// Output path for corpus file
        #[arg(short, long)]
        output: PathBuf,
    },
    
    /// List supported types
    List,
    
    /// Validate all types in a directory
    ValidateAll {
        /// Directory containing corpus files
        #[arg(short, long)]
        corpus_dir: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Import { type_name, corpus_file } => {
            println!("Importing {} from {:?}", type_name, corpus_file);
            corpus::import_corpus(&type_name, &corpus_file)?;
            println!("✓ Validation successful");
        }
        
        Commands::Export { type_name, output } => {
            println!("Exporting {} to {:?}", type_name, output);
            corpus::export_corpus(&type_name, &output)?;
            println!("✓ Export successful");
        }
        
        Commands::List => {
            println!("Supported types:");
            for type_name in registry::list_types() {
                println!("  - {}", type_name);
            }
        }
        
        Commands::ValidateAll { corpus_dir } => {
            println!("Validating all types in {:?}", corpus_dir);
            let count = corpus::validate_all(&corpus_dir)?;
            println!("✓ Validated {} type(s) successfully", count);
        }
    }
    
    Ok(())
}
