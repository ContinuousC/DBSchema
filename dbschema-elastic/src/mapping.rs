/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use dbschema::{
    BoolSchema, DateTimeSchema, DbSchema, DictionarySchema, DoubleSchema,
    EnumFormat, EnumSchema, IntegerSchema, Ipv4Schema, Ipv6Schema, JsonSchema,
    ListSchema, OptionSchema, SetSchema, StringSchema, StructSchema,
    UnitSchema,
};

use super::error::MappingError;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(transparent)]
pub struct ElasticMapping(Value);

impl ElasticMapping {
    pub fn new(schema: &DbSchema) -> Result<Self, MappingError> {
        Ok(ElasticMapping(match schema {
            DbSchema::Struct(StructSchema {
                fields,
                flatten,
                elastic_rename,
                ..
            }) => {
                let fields = Value::Object(
                    fields
                        .iter()
                        .try_fold(Map::new(), |mut fields, (field, schema)| {
                            match flatten.contains(field) {
                                false => {
									let es_name = elastic_rename.get(field).unwrap_or(field);
									fields.insert(es_name.to_string(), Self::new(schema)?.0);
									Ok(fields)
                                },
                                true => match schema {
                                    DbSchema::Struct(_) | DbSchema::Enum(_) => {
                                        match Self::new(schema)?.0 {
                                            Value::Object(mut map) => {
                                                match map.remove("properties") {
                                                    Some(Value::Object(
                                                        props,
                                                    )) => {
														fields.extend(props);
														Ok(fields)
													}
                                                    _ => Err(
                                                        MappingError::Internal(
                                                            "flattened struct mappings is missing the 'properties' field \
															 or the field is not an object",
                                                        ),
                                                    ),
                                                }
											},
                                            _ => Err(
                                                MappingError::Internal("flattened struct mappings is not an object"),
                                            ),
                                        }
                                    },
									_ => Err(MappingError::FlattenNonStruct)
                                },
                            }
                        })?
                );
                Ok(json!({ "properties": fields }))
            }
            DbSchema::Dictionary(DictionarySchema { value_type, .. }) => {
                Ok(json!({
                    "properties": {
                        "key": Self::new(&StringSchema::new().into())?.0,
                        "value": Self::new(value_type)?.0
                    }
                }))
            }
            DbSchema::List(ListSchema { value_type, .. })
            | DbSchema::Set(SetSchema { value_type, .. }) => {
                Ok(Self::new(value_type)?.0)
            }
            DbSchema::Option(OptionSchema { value_type, .. }) => {
                Ok(Self::new(value_type)?.0)
            }
            DbSchema::Enum(EnumSchema {
                options, format, ..
            }) => match format {
                EnumFormat::ExternallyTagged | EnumFormat::UnitAsString => {
                    let option_mapping = Value::Object(
                        options
                            .iter()
                            .map(|(k, v)| Ok((k.to_string(), Self::new(v)?.0)))
                            .collect::<Result<_, _>>()?,
                    );
                    Ok(json!({ "properties": option_mapping }))
                }
                EnumFormat::TagString => Ok(json!({
                    "type": "keyword",
                    "ignore_above": 1024
                })),
            },
            DbSchema::DateTime(DateTimeSchema { .. }) => Ok(json!({
                "type": "date",
                "format": "strict_date_time"
            })),
            DbSchema::String(StringSchema { .. }) => Ok(json!({
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 1024
                    }
                }
            })),
            DbSchema::Ipv4(Ipv4Schema { .. }) => Ok(json!({"type": "ip"})),
            DbSchema::Ipv6(Ipv6Schema { .. }) => Ok(json!({"type": "ip"})),
            DbSchema::Integer(IntegerSchema { .. }) => {
                Ok(json!({"type": "long"}))
            }
            DbSchema::Double(DoubleSchema { .. }) => {
                Ok(json!({"type": "double"}))
            }
            DbSchema::Bool(BoolSchema { .. }) => Ok(json!({"type": "boolean"})),
            DbSchema::Json(JsonSchema { .. }) => {
                Ok(json!({"type": "object", "enabled": false}))
            }
            DbSchema::Unit(UnitSchema { .. }) => Ok(json!({"type": "object"})),
        }?))
    }
}

impl fmt::Display for ElasticMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string_pretty(&self.0).map_err(|_| fmt::Error)?
        )
    }
}
