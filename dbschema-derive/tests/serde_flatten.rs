/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use serde::{Deserialize, Serialize};

use dbschema::{
    DbSchema, HasSchema, IntegerSchema, StringSchema, StructSchema,
};
use serde_json::json;

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Named<T> {
    name: String,
    #[serde(flatten)]
    value: T,
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Thing {
    r#type: String,
    value: i64,
}

#[test]
fn expected_schema() {
    let expected: DbSchema = StructSchema::new()
        .field("name", StringSchema::new())
        .field("type", StringSchema::new())
        .field("value", IntegerSchema::new())
        .into();
    assert!(Named::<Thing>::schema() == expected);
}

#[test]
fn serialize() {
    let value: Named<Thing> = Named {
        name: "some name".to_string(),
        value: Thing {
            r#type: "everything".to_string(),
            value: 42,
        },
    };
    let expected = json!({
        "name": "some name",
        "type": "everything",
        "value": 42
    });
    assert!(serde_json::to_value(value).unwrap() == expected)
}

#[test]
fn verify_value() {
    let value = json!({
        "name": "some name",
        "type": "everything",
        "value": 42
    });
    Named::<Thing>::schema().verify_value(&value).unwrap();
}

#[test]
fn reject_invalid_value1() {
    let value = json!({
        "name": "some name",
        "type": "everything",
    });
    assert_eq!(
        Named::<Thing>::schema()
            .verify_value(&value)
            .unwrap_err()
            .to_string(),
        "Missing field: value"
    )
}

#[test]
fn reject_invalid_value2() {
    let value = json!({
        "name": "some name",
        "type": "everything",
        "value": 42,
        "extra": "oops"
    });
    assert_eq!(
        Named::<Thing>::schema()
            .verify_value(&value)
            .unwrap_err()
            .to_string(),
        "Unknown field: extra"
    );
}
