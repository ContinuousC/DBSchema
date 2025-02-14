/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use dbschema::{
    EnumSchema, HasSchema, IntegerSchema, StringSchema, UnitSchema,
};
use serde_json::json;

#[derive(HasSchema)]
#[dbschema(format = "tag_string")]
enum Status {
    #[allow(unused)]
    Ok,
    #[allow(unused)]
    Warn,
    #[allow(unused)]
    Crit,
}

#[test]
fn result_set_default() {
    let expected = EnumSchema::new()
        .option("Ok", IntegerSchema::new().default(42))
        .option("Err", StringSchema::new())
        .default("Ok")
        .into();
    let schema = Result::<i64, String>::schema()
        .set_default(json!({"Ok": 42}))
        .unwrap();
    assert_eq!(schema, expected);
}

#[test]
fn result_status_default() {
    let expected = EnumSchema::new()
        .option(
            "Ok",
            EnumSchema::new()
                .option("Ok", UnitSchema::new())
                .option("Warn", UnitSchema::new())
                .option("Crit", UnitSchema::new())
                .default("Ok")
                .tag_string(),
        )
        .option("Err", StringSchema::new())
        .default("Ok")
        .into();
    eprintln!("{expected:?}");
    let schema = Result::<Status, String>::schema()
        .set_default(json!({"Ok": "Ok"}))
        .unwrap();
    assert_eq!(schema, expected);
}
