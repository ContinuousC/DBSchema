/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fs::File;
use std::path::{Path, PathBuf};
use std::{io, process};

use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;
use serde_json::Value;
//use serde_json::Value;
use thiserror::Error;

use dbschema::{DbSchema, DbTable, Filter};
use dbschema_elastic as elastic;

/// Test commands for the elastic backend of the ContinuousC DBDaemon.
#[derive(Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Mappings(MappingsArgs),
    Filter(FilterArgs),
    Load(LoadArgs),
    Save(SaveArgs),
}

#[derive(Parser, Debug)]
struct MappingsArgs {
    /// Take table definitions and values as input.
    #[clap(long, short)]
    table: bool,
    /// The path to the schema file.
    schema: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct FilterArgs {
    /// Take table definitions and values as input.
    #[clap(long, short)]
    table: bool,
    /// The path to the schema file.
    schema: PathBuf,
    /// The path to the filter file.
    filter: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct LoadArgs {
    /// Take table definitions and values as input.
    #[clap(long, short)]
    table: bool,
    /// The path to the schema file.
    schema: PathBuf,
    /// The path to the value file.
    value: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct SaveArgs {
    /// Take table definitions and values as input.
    #[clap(long, short)]
    table: bool,
    /// The path to the schema file.
    schema: PathBuf,
    /// The path to the value file.
    value: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Mappings(args) => {
            match mappings(args.schema.as_deref(), args.table) {
                Ok(mapping) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&mapping).unwrap()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
        Command::Filter(args) => {
            match filter(&args.schema, args.filter.as_deref(), args.table) {
                Ok(filter) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&filter).unwrap()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
        Command::Load(args) => {
            match load(&args.schema, args.value.as_deref(), args.table) {
                Ok(value) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&value).unwrap()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
        Command::Save(args) => {
            match save(&args.schema, args.value.as_deref(), args.table) {
                Ok(value) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&value).unwrap()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
    }
}

fn mappings(
    schema: Option<&Path>,
    is_table: bool,
) -> Result<elastic::ElasticMapping> {
    let schema = read_schema_file(schema, is_table)?;
    Ok(elastic::ElasticMapping::new(&schema)?)
}

fn filter(
    schema: &Path,
    filter: Option<&Path>,
    is_table: bool,
) -> Result<elastic::ElasticFilter> {
    let schema = read_schema_file(Some(schema), is_table)?;
    let filter: Filter = read_filter_file(filter)?;
    Ok(elastic::ElasticFilter::new(&schema, &filter)?)
}

fn load(schema: &Path, value: Option<&Path>, is_table: bool) -> Result<Value> {
    let schema = read_schema_file(Some(schema), is_table)?;
    let value: elastic::ElasticValue = read_value_file(value)?;
    Ok(value.load(&schema)?)
}

fn save(
    schema: &Path,
    value: Option<&Path>,
    is_table: bool,
) -> Result<elastic::ElasticValue> {
    let schema = read_schema_file(Some(schema), is_table)?;
    let value: Value = read_value_file(value)?;
    Ok(elastic::ElasticValue::save(&schema, value)?)
}

fn read_schema_file(path: Option<&Path>, is_table: bool) -> Result<DbSchema> {
    match is_table {
        true => Ok(read_file::<DbTable, _, _>(
            path,
            Error::ReadTable,
            Error::TableFormat,
        )?
        .schema()),
        false => read_file(path, Error::ReadSchema, Error::SchemaFormat),
    }
}

fn read_filter_file(path: Option<&Path>) -> Result<Filter> {
    read_file(path, Error::ReadFilter, Error::FilterFormat)
}

fn read_value_file<T: DeserializeOwned>(path: Option<&Path>) -> Result<T> {
    read_file(path, Error::ReadValue, Error::ValueFormat)
}

fn read_file<T: DeserializeOwned, F, G>(
    path: Option<&Path>,
    read_err: F,
    deserialize_err: G,
) -> Result<T>
where
    F: FnOnce(io::Error) -> Error,
    G: FnOnce(serde_json::Error) -> Error,
{
    match path {
        Some(path) => serde_json::from_reader(io::BufReader::new(
            File::open(path).map_err(read_err)?,
        )),
        None => serde_json::from_reader(io::stdin()),
    }
    .map_err(deserialize_err)
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
enum Error {
    #[error("Failed to read table definition: {0}")]
    ReadTable(io::Error),
    #[error("Failed to read schema: {0}")]
    ReadSchema(io::Error),
    #[error("Failed to read filter: {0}")]
    ReadFilter(io::Error),
    #[error("Failed to read value: {0}")]
    ReadValue(io::Error),
    #[error("Table format error: {0}")]
    TableFormat(serde_json::Error),
    #[error("Schema format error: {0}")]
    SchemaFormat(serde_json::Error),
    #[error("Filter format error: {0}")]
    FilterFormat(serde_json::Error),
    #[error("Value format error: {0}")]
    ValueFormat(serde_json::Error),
    #[error("Mapping conversion error: {0}")]
    Mapping(#[from] elastic::MappingError),
    #[error("Filter conversion error: {0}")]
    ElasticFilter(#[from] elastic::FilterError),
    #[error("Value conversion error: {0}")]
    ElasticConversion(#[from] elastic::ConversionError),
}
