/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use clap::Parser;
use serde_json::Value;

use dbschema::DbSchema;

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    schema: PathBuf,
    origin: PathBuf,
    other: PathBuf,
}

fn main() {
    let args = Args::parse();
    let schema: DbSchema = serde_json::from_reader(BufReader::new(
        File::open(&args.schema).unwrap(),
    ))
    .unwrap();
    let origin: Value = serde_json::from_reader(BufReader::new(
        File::open(&args.origin).unwrap(),
    ))
    .unwrap();
    let other: Value = serde_json::from_reader(BufReader::new(
        File::open(&args.other).unwrap(),
    ))
    .unwrap();

    println!(
        "{}",
        serde_json::to_string_pretty(&schema.diff(&origin, &other).unwrap())
            .unwrap()
    );
}
