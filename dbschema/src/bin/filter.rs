/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::{fs, path::PathBuf, process};

use clap::Parser;
use serde_json::Value;

use dbschema::{DbSchema, Filter};

/// Test DbDaemon filtering.
#[derive(Parser)]
struct Args {
    schema: PathBuf,
    filter: PathBuf,
    value: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();

    let schema: DbSchema = serde_json::from_str(
        &fs::read_to_string(&args.schema).expect("failed to read schema"),
    )
    .expect("failed to decode schema");

    let filter: Filter = serde_json::from_str(
        &fs::read_to_string(&args.filter).expect("failed to read filter"),
    )
    .expect("failed to decode filter");

    schema.verify().expect("schema failed to verify");

    process::exit(match &args.value {
        Some(value) => {
            let value: Value = serde_json::from_str(
                &fs::read_to_string(value).expect("failed to read value"),
            )
            .expect("failed to decode value");

            match filter.matches(&schema, &value) {
                Ok(r) => match r {
                    true => {
                        println!("The filter matches the value!");
                        0
                    }
                    false => {
                        println!("The filter doesn't match the value!");
                        1
                    }
                },
                Err(e) => {
                    println!("Error: {e}");
                    2
                }
            }
        }
        None => match filter.verify(&schema) {
            Ok(()) => {
                println!("The filter is correct!");
                0
            }
            Err(e) => {
                println!("Error: {e}");
                1
            }
        },
    })
}
