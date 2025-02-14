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
    #[dbschema(flatten)]
    value: T,
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Thing {
    r#type: String,
    value: i64,
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct OldThing {
    name: String,
    r#type: String,
    value: i64,
}

#[test]
fn expected_schema() {
    let expected: DbSchema = StructSchema::new()
        .field("name", StringSchema::new())
        .field(
            "value",
            StructSchema::new()
                .field("type", StringSchema::new())
                .field("value", IntegerSchema::new()),
        )
        .flatten("value")
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
        "value": {
            "type": "everything",
            "value": 42
        }
    });
    assert!(serde_json::to_value(value).unwrap() == expected)
}

#[test]
fn compatibility() {
    let old_schema = OldThing::schema();
    let new_schema = Named::<Thing>::schema();
    new_schema
        .verify_backward_compatibility(&old_schema)
        .unwrap()
}
