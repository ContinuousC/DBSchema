/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use serde::{Deserialize, Serialize};

use dbschema::{
    BoolSchema, DbSchema, DoubleSchema, EnumSchema, HasSchema, IntegerSchema,
    StringSchema, StructSchema, UnitSchema,
};

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct Config {
    #[serde(default)]
    structure: MyStruct,
    #[serde(default)]
    enumeration: MyEnum,
    #[serde(default)]
    boolean: bool,
    #[serde(default)]
    string: String,
    #[serde(default)]
    integer: i64,
    #[serde(default)]
    double: f64,
    #[serde(default)]
    unit: (),
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
struct MyStruct {
    field1: String,
    field2: i64,
}

impl Default for MyStruct {
    fn default() -> Self {
        Self {
            field1: "Hello world!".to_string(),
            field2: 42,
        }
    }
}

#[derive(HasSchema, Serialize, Deserialize, Debug)]
enum MyEnum {
    Choice1(String),
    Choice2(i64),
}

impl Default for MyEnum {
    fn default() -> Self {
        Self::Choice1("something".to_string())
    }
}

#[test]
fn expected_schema() {
    let expected: DbSchema = StructSchema::new()
        .field(
            "structure",
            StructSchema::new()
                .field("field1", StringSchema::new().default("Hello world!"))
                .field("field2", IntegerSchema::new().default(42)),
        )
        .field(
            "enumeration",
            EnumSchema::new()
                .option(
                    "Choice1",
                    StringSchema::new().default("something".to_string()),
                )
                .option("Choice2", IntegerSchema::new())
                .default("Choice1")
                .unit_as_string(),
        )
        .field("boolean", BoolSchema::new().default(false))
        .field("string", StringSchema::new().default(""))
        .field("integer", IntegerSchema::new().default(0))
        .field("double", DoubleSchema::new().default(0.0))
        .field("unit", UnitSchema::new())
        .into();
    assert!(Config::schema() == expected);
}
