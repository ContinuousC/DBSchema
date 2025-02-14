/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use serde::{Deserialize, Serialize};

use dbschema::{
    DbSchema, HasSchema, IntegerSchema, StringSchema, StructSchema,
};

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Timestamped<T> {
    #[dbschema(elastic_rename = "@timestamp")]
    timestamp: String,
    #[dbschema(flatten)]
    value: T,
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Metric {
    value: i64,
}

#[test]
fn expected_schema() {
    let expected: DbSchema = StructSchema::new()
        .field("timestamp", StringSchema::new())
        .field(
            "value",
            StructSchema::new().field("value", IntegerSchema::new()),
        )
        .flatten("value")
        .elastic_rename("timestamp", "@timestamp")
        .into();
    assert!(Timestamped::<Metric>::schema() == expected);
}
