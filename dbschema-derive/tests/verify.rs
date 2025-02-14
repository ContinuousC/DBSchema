/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use serde::{Deserialize, Serialize};

use dbschema::{
    DbSchema, EnumSchema, HasSchema, IntegerSchema, ListSchema, StringSchema,
    StructSchema, UnitSchema,
};

use serde_json::json;

#[derive(HasSchema, Serialize, Deserialize, Debug)]
#[dbschema(removed = "xxx", removed = "yyy")]
struct Person {
    name: Name,
    gender: Gender,
    age: i64,
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum Gender {
    Male,
    Female,
    #[serde(rename = "something_else")]
    Other(String),
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Name {
    first: Vec<String>,
    last: Vec<String>,
}

#[test]
fn expected_schema() {
    let expected: DbSchema = StructSchema::new()
        .field(
            "name",
            StructSchema::new()
                .field("first", ListSchema::new(StringSchema::new()))
                .field("last", ListSchema::new(StringSchema::new())),
        )
        .field(
            "gender",
            EnumSchema::new()
                .option("male", UnitSchema::new())
                .option("female", UnitSchema::new())
                .option("something_else", StringSchema::new())
                .unit_as_string(),
        )
        .field("age", IntegerSchema::new())
        .removed("xxx")
        .removed("yyy")
        .into();
    assert!(Person::schema() == expected);
}

#[test]
fn schema_verifies_value() {
    let schema = Person::schema();
    let instance = Person {
        name: Name {
            first: vec!["Bill".to_string()],
            last: vec!["Gates".to_string()],
        },
        gender: Gender::Male,
        age: 111,
    };
    let val = serde_json::to_value(instance).unwrap();
    schema
        .verify_value(&val)
        .expect("failed to verify serialized value");
}

#[test]
fn schema_rejects_invalid() {
    let schema = Person::schema();
    let invalid = json!({
        "name": {
            "first": ["Bill"],
            "last": ["Gates"]
        },
        "age": "old"
    });
    assert!(
        schema.verify_value(&invalid).is_err(),
        "Schema failed to reject an invalid value"
    )
}
