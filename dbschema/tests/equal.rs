/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use dbschema::{
    DbSchema, IntegerSchema, ListSchema, OptionSchema, SetSchema, StringSchema,
    StructSchema,
};
use serde_json::json;

#[test]
fn set_equal() {
    let schema: DbSchema = SetSchema::new(StringSchema::new()).into();
    assert!(schema
        .value_eq(&json!(["abc", "def"]), &json!(["def", "abc"]))
        .unwrap());
    assert!(!schema
        .value_eq(&json!(["abc", "def"]), &json!(["def", "abc", "ghi"]))
        .unwrap());
    assert!(!schema
        .value_eq(&json!(["abc", "def"]), &json!(["abc", "abc", "def"]))
        .unwrap());
}

#[test]
fn list_equal() {
    let schema: DbSchema = ListSchema::new(StringSchema::new()).into();
    assert!(schema
        .value_eq(&json!(["abc", "def"]), &json!(["abc", "def"]))
        .unwrap());
    assert!(!schema
        .value_eq(&json!(["abc", "def"]), &json!(["def", "abc"]))
        .unwrap());
    assert!(!schema
        .value_eq(&json!(["abc", "def"]), &json!(["abc", "def", "ghi"]))
        .unwrap());
}

#[test]
fn struct_equal() {
    let schema: DbSchema = StructSchema::new()
        .field("name", StringSchema::new())
        .field("age", OptionSchema::new(IntegerSchema::new()))
        .into();
    assert!(schema
        .value_eq(
            &json!({"name": "George", "age": 42}),
            &json!({"name": "George", "age": 42, "extra": null})
        )
        .unwrap());
    assert!(!schema
        .value_eq(
            &json!({"name": "George", "age": 42}),
            &json!({"name": "George", "age": null})
        )
        .unwrap());
}
