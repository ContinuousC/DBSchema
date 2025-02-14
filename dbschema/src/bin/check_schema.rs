/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use clap::Parser;
use serde::de::DeserializeOwned;
use thiserror::Error;

use dbschema::{DbSchema, DbTable};

#[derive(Parser, Debug)]
struct Args {
    /// Verify tables instead of schemas.
    #[clap(short, long)]
    table: bool,
    files: Vec<PathBuf>,
}

fn main() {
    let args = Args::parse();
    for path in &args.files {
        let res = match args.table {
            true => check::<DbTable>(path),
            false => check::<DbSchema>(path),
        };
        match res {
            Ok(()) => println!("Schema '{}' is OK", path.display()),
            Err(e) => {
                println!("Schema '{}' is not OK: {:?}", path.display(), e)
            }
        }
    }
}

fn check<T>(path: &Path) -> Result<(), Error>
where
    T: DeserializeOwned,
{
    let _ = serde_json::from_reader::<_, T>(File::open(path)?)?;
    Ok(())
}

#[derive(Error, Debug)]
enum Error {
    #[error("failed to open file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to deserialize: {0}")]
    Deserialize(#[from] serde_json::Error),
}
