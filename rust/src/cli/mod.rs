//! Command-line interface for Skypydb.

use crate::errors::{Result, SkypydbError};
use crate::security::EncryptionManager;
use crate::server::run_dashboard_server;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use clap::{Parser, Subcommand};
use dialoguer::Select;
use reqwest::Client;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Component, Path, PathBuf};
use zip::ZipArchive;

const DASHBOARD_ZIP_URL: &str = "https://github.com/Ahen-Studio/the-skypydb-dashboard/archive/refs/heads/main.zip";
const DASHBOARD_FOLDER_NAME: &str = "dashboard";

#[derive(Debug, Parser)]
#[command(
    name = "skypydbrust",
    about = "Skypydb CLI - Reactive and Vector Embedding Database",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Interactive mode.
    Dev,

    /// Initialize project files and encryption keys.
    Init,

    /// Print a new encryption key and salt.
    Keygen,

    /// Start dashboard API server.
    Dashboard {
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value_t = 8000)]
        port: u16,
    },
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Commands::Dev) {
        Commands::Dev => run_dev().await,
        Commands::Init => init_project().await,
        Commands::Dashboard { host, port } => {
            println!("Starting Skypydb dashboard API on http://{host}:{port}");
            run_dashboard_server(&host, port).await
        }
        Commands::Keygen => {
            print_keys()?;

            Ok(())
        }
    }
}

async fn run_dev() -> Result<()> {
    let options = vec![
        "create a new project",
        "launch the dashboard api server",
        "no thanks",
    ];
    let choice = Select::new()
        .with_prompt("Welcome to Skypydb! What would you like to do?")
        .items(&options)
        .default(0)
        .interact_opt()
        .map_err(|error| SkypydbError::database(error.to_string()))?;

    match choice {
        Some(0) => init_project().await,
        Some(1) => {
            println!("Starting Skypydb dashboard API on http://0.0.0.0:8000");
            run_dashboard_server("0.0.0.0", 8000).await
        }
        _ => {
            println!("Exiting.");

            Ok(())
        }
    }
}

/// Initializes a new Skypydb project. Creates the db directory and writes the schema.rs file.
async fn init_project() -> Result<()> {
    let cwd = std::env::current_dir().map_err(|error| SkypydbError::database(error.to_string()))?;
    let db_dir = cwd.join("db");
    let generated_dir = db_dir.join("_generated");
    let schema_file = db_dir.join("schema.rs");

    fs::create_dir_all(&generated_dir)
        .map_err(|error| SkypydbError::database(error.to_string()))?;

    if !schema_file.exists() {
        fs::write(&schema_file, schema_template())
            .map_err(|error| SkypydbError::database(error.to_string()))?;
    }

    write_env_file(&cwd)?;
    update_gitignore(&cwd)?;
    download_dashboard_folder(&generated_dir).await?;

    println!("Initialized project.");
    println!("Write your Skypydb schema in {}", schema_file.display());
    println!("Use `skypydbrust dashboard` to run the dashboard API server.");

    Ok(())
}

/// Writes the encryption key and salt to the .env.local file. Use by the init command.
fn write_env_file(cwd: &Path) -> Result<()> {
    let encryption_key = EncryptionManager::generate_key();
    let salt = EncryptionManager::generate_salt(32)?;
    let content = format!(
        "ENCRYPTION_KEY={encryption_key}\nSALT_KEY={}\n",
        BASE64_STANDARD.encode(salt)
    );
    let env_path = cwd.join(".env.local");
    fs::write(env_path, content).map_err(|error| SkypydbError::database(error.to_string()))
}

/// Updates the .gitignore file to include the .env.local file. Use by the init command.
fn update_gitignore(cwd: &Path) -> Result<()> {
    let gitignore_path = cwd.join(".gitignore");
    let mut lines = if gitignore_path.exists() {
        fs::read_to_string(&gitignore_path)
            .map_err(|error| SkypydbError::database(error.to_string()))?
            .lines()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if !lines.iter().any(|line| line.trim() == ".env.local") {
        lines.push(".env.local".to_string());
        let mut file = fs::File::create(&gitignore_path)
            .map_err(|error| SkypydbError::database(error.to_string()))?;
        let body = if lines.is_empty() {
            String::new()
        } else {
            format!("{}\n", lines.join("\n"))
        };
        file.write_all(body.as_bytes())
            .map_err(|error| SkypydbError::database(error.to_string()))?;
    }

    Ok(())
}

/// Returns the schema template. Use by the init command.
fn schema_template() -> String {
    let mut lines = Vec::<String>::new();
    lines.push("//Write your Skypydb schema in this file.".to_string());
    lines.join("\n")
}

/// Downloads the dashboard folder from GitHub and writes it to db/_generated/dashboard. Use by the init command.
async fn download_dashboard_folder(generated_dir: &Path) -> Result<()> {
    let target_root = generated_dir.join(DASHBOARD_FOLDER_NAME);
    let client = Client::builder()
        .user_agent("skypydbrust-cli")
        .build()
        .map_err(|error| SkypydbError::database(error.to_string()))?;
    let response = match client.get(DASHBOARD_ZIP_URL).send().await {
        Ok(response) => response,
        Err(error) => {
            println!("Warning: failed to download dashboard folder: {error}");
            return Ok(());
        }
    };
    if !response.status().is_success() {
        println!(
            "Warning: failed to download dashboard folder. HTTP status: {}",
            response.status()
        );
        return Ok(());
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| SkypydbError::database(error.to_string()))?;
    let mut archive = ZipArchive::new(Cursor::new(bytes))
        .map_err(|error| SkypydbError::database(error.to_string()))?;
    let mut created_files = 0_usize;
    let mut skipped_files = 0_usize;

    'entry_loop: for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| SkypydbError::database(error.to_string()))?;
        let normalized = entry.name().replace('\\', "/");
        let mut parts = normalized.split('/').filter(|part| !part.is_empty());
        let _repo_root = match parts.next() {
            Some(value) => value,
            None => continue,
        };
        let dashboard_part = match parts.next() {
            Some(value) => value,
            None => continue,
        };
        if dashboard_part != DASHBOARD_FOLDER_NAME {
            continue;
        }
        let relative_parts = parts.collect::<Vec<_>>();
        if relative_parts.is_empty() {
            continue;
        }
        let mut relative_path = PathBuf::new();

        for part in &relative_parts {
            if part.is_empty() {
                continue 'entry_loop;
            }
            let mut part_components = Path::new(part).components();
            match part_components.next() {
                Some(Component::Normal(_)) => {}
                _ => continue 'entry_loop,
            }
            if part_components.next().is_some() || part.contains('\0') {
                continue 'entry_loop;
            }
            relative_path.push(part);
        }
        if relative_path.as_os_str().is_empty() {
            continue;
        }
        let target_path = target_root.join(&relative_path);
        if !target_path.starts_with(&target_root) {
            continue;
        }
        if entry.is_dir() {
            fs::create_dir_all(&target_path)
                .map_err(|error| SkypydbError::database(error.to_string()))?;
            continue;
        }
        if target_path.exists() {
            skipped_files += 1;
            continue;
        }
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| SkypydbError::database(error.to_string()))?;
        }
        let mut output_file = fs::File::create(&target_path)
            .map_err(|error| SkypydbError::database(error.to_string()))?;
        std::io::copy(&mut entry, &mut output_file)
            .map_err(|error| SkypydbError::database(error.to_string()))?;
        created_files += 1;
    }

    if created_files > 0 {
        println!("Downloaded dashboard folder to {}", target_root.display());
    }
    if skipped_files > 0 {
        println!(
            "Skipped {skipped_files} existing file(s) in {}",
            target_root.display()
        );
    }

    Ok(())
}

/// Prints the encryption key and salt to the console. Use by the keygen command.
fn print_keys() -> Result<()> {
    let encryption_key = EncryptionManager::generate_key();
    let salt = EncryptionManager::generate_salt(32)?;

    println!("ENCRYPTION_KEY={encryption_key}");
    println!("SALT_KEY={}", BASE64_STANDARD.encode(salt));

    Ok(())
}
