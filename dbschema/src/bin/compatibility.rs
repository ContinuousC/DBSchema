/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::{
    fs,
    path::{Path, PathBuf},
    process,
};

use clap::Parser;
use dbschema::{Compatibility, DbSchema};
use thiserror::Error;

/// Verify DbSchema compatibility.
#[derive(clap::Parser)]
struct Args {
    /// Path to current schema.
    current: PathBuf,
    /// Path to previous schema.
    previous: PathBuf,
}

fn main() {
    let args = Args::parse();

    match run(&args.current, &args.previous) {
        Ok(Compatibility::Compatible) => {
            eprintln!("The schema is backward compatible.");
        }
        Ok(Compatibility::NeedsReindex) => {
            eprintln!("A reindex is required.");
        }
        Err(err) => {
            eprintln!("Error: {err}");
            process::exit(err.exit_code());
        }
    }
}

fn run(
    current_schema_path: &Path,
    previous_schema_path: &Path,
) -> Result<Compatibility, Error> {
    let current_schema: DbSchema = serde_json::from_str(
        &fs::read_to_string(current_schema_path)
            .map_err(Error::ReadCurrentSchema)?,
    )
    .map_err(Error::DeserializeCurrentSchema)?;

    let previous_schema: DbSchema = serde_json::from_str(
        &fs::read_to_string(previous_schema_path)
            .map_err(Error::ReadPreviousSchema)?,
    )
    .map_err(Error::DeserializePreviousSchema)?;

    current_schema
        .verify()
        .map_err(Error::VerifyCurrentSchema)?;
    previous_schema
        .verify()
        .map_err(Error::VerifyPreviousSchema)?;

    current_schema
        .verify_compatibility(&previous_schema)
        .map_err(Error::VerifyCompatibility)
}

#[derive(Error, Debug)]
enum Error {
    #[error("failed to read current schema: {0}")]
    ReadCurrentSchema(std::io::Error),
    #[error("failed to read previous schema: {0}")]
    ReadPreviousSchema(std::io::Error),
    #[error("failed to deserialize current schema: {0}")]
    DeserializeCurrentSchema(serde_json::Error),
    #[error("failed to deserialize previous schema: {0}")]
    DeserializePreviousSchema(serde_json::Error),
    #[error("failed to verify current schema: {0}")]
    VerifyCurrentSchema(dbschema::Error),
    #[error("failed to verify previous schema: {0}")]
    VerifyPreviousSchema(dbschema::Error),
    #[error("failed to verify backward compatibility: {0}")]
    VerifyCompatibility(dbschema::Error),
}

impl Error {
    fn exit_code(&self) -> i32 {
        match self {
            Error::ReadCurrentSchema(_) | Error::ReadPreviousSchema(_) => 3,
            Error::DeserializeCurrentSchema(_)
            | Error::DeserializePreviousSchema(_) => 2,
            Error::VerifyCurrentSchema(_)
            | Error::VerifyPreviousSchema(_)
            | Error::VerifyCompatibility(_) => 1,
        }
    }
}
