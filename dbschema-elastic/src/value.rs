/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::{
    iter::once,
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Number, Value};

use dbschema::EnumFormat;
use dbschema::{
    DbSchema, DictionarySchema, EnumSchema, ListSchema, OptionSchema,
    SetSchema, StructSchema, ValueExt,
};

use super::ConversionError;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ElasticValue(Value);

impl ElasticValue {
    /// Convert a value from database representation to in-memory representation.
    pub fn load(self, schema: &DbSchema) -> Result<Value, ConversionError> {
        match schema {
            // {"field1": es_value, ...} => {"field1": value, ...}
            DbSchema::Struct(s) => {
                let mut obj = match self.0 {
                    Value::Object(map) => Ok(map),
                    _ => Err(ConversionError::InvalidJsonValue(
                        "object",
                        self.0.value_type(),
                    )),
                }?;
                load_struct(s, &mut obj)
            }
            // [{"$key": "K", "&value": EV}] => {"K": V}
            DbSchema::Dictionary(DictionarySchema { value_type, .. }) => {
                let array = match self.0 {
                    Value::Array(array) => Ok(array),
                    _ => Err(ConversionError::InvalidJsonValue(
                        "array",
                        self.0.value_type(),
                    )),
                }?;
                Ok(Value::Object(
                    array
                        .into_iter()
                        .map(|elem| {
                            let mut map = match elem {
                                Value::Object(map) => Ok(map),
                                _ => Err(ConversionError::InvalidJsonValue(
                                    "object",
                                    elem.value_type(),
                                )),
                            }?;
                            let key = match map.remove("key") {
                                Some(Value::String(key)) => Ok(key),
                                Some(value) => {
                                    Err(ConversionError::InvalidJsonValue(
                                        "string",
                                        value.value_type(),
                                    )
                                    .add_path("(key)"))
                                }
                                None => Err(ConversionError::MissingField(
                                    "key".to_string(),
                                )),
                            }?;
                            let value =
                                Self(map.remove("value").ok_or_else(|| {
                                    ConversionError::MissingField(
                                        "value".to_string(),
                                    )
                                })?)
                                .load(value_type)
                                .map_err(
                                    |e| e.add_path(&key).add_path("value"),
                                )?;
                            Ok((key, value))
                        })
                        .collect::<Result<_, ConversionError>>()?,
                ))
            }
            // [EV] => [V]
            DbSchema::List(ListSchema {
                value_type,
                min_length,
                max_length,
            }) => test_json_arr(self.0, *min_length, *max_length)?
                .into_iter()
                .enumerate()
                .map(|(i, val)| {
                    Self(val).load(value_type).map_err(|e| e.add_path(i))
                })
                .collect(),
            DbSchema::Set(SetSchema {
                value_type,
                min_length,
                max_length,
            }) => test_json_arr(self.0, *min_length, *max_length)?
                .into_iter()
                .try_fold(Vec::new(), |mut set, elem| {
                    match set.contains(&elem) {
                        false => {
                            set.push(elem);
                            Ok(set)
                        }
                        true => Err(ConversionError::DuplicateValueInSet(elem)),
                    }
                })?
                .into_iter()
                .enumerate()
                .map(|(i, val)| {
                    Self(val)
                        .load(value_type)
                        .map_err(|e| e.add_path(i.to_string()))
                })
                .collect(),
            // None => None || EV => V
            DbSchema::Option(OptionSchema { value_type, .. }) => match self.0 {
                Value::Null => Ok(Value::Null),
                _ => Self(self.0)
                    .load(value_type)
                    .map_err(|e| e.add_path("some")),
            },
            // {"tag": EV} => {"tag": V}
            DbSchema::Enum(EnumSchema {
                options, format, ..
            }) => {
                let (tag, value) = match self.0 {
                    Value::Object(map)
                        if (*format == EnumFormat::ExternallyTagged
                            || *format == EnumFormat::UnitAsString)
                            && map.len() == 1 =>
                    {
                        Ok(map.into_iter().next().unwrap())
                    }
                    Value::String(s) if *format == EnumFormat::TagString => {
                        Ok((s, Value::Null))
                    }
                    _ => Err(ConversionError::InvalidEnumValue),
                }?;
                let option_schema = options.get(&tag).ok_or_else(|| {
                    ConversionError::MissingOption(tag.to_string())
                })?;
                let val = Self(value)
                    .load(option_schema)
                    .map_err(|e| e.add_path(&tag))?;
                match *format == EnumFormat::TagString
                    || *format == EnumFormat::UnitAsString && val.is_null()
                {
                    true => Ok(json!(tag)),
                    false => Ok(Value::Object(once((tag, val)).collect())),
                }
            }
            DbSchema::Ipv4(_) => match self.0 {
                Value::String(s) => match Ipv4Addr::from_str(&s) {
                    Ok(_) => Ok(Value::String(s)),
                    Err(e) => Err(ConversionError::InvalidIpv4Address(s, e)),
                },
                _ => Err(ConversionError::InvalidJsonValue(
                    "string",
                    self.0.value_type(),
                )),
            },
            DbSchema::Ipv6(_) => match self.0 {
                Value::String(s) => match Ipv6Addr::from_str(&s) {
                    Ok(_) => Ok(Value::String(s)),
                    Err(e) => Err(ConversionError::InvalidIpv6Address(s, e)),
                },
                _ => Err(ConversionError::InvalidJsonValue(
                    "string",
                    self.0.value_type(),
                )),
            },
            // EV => Datatime<Utc> => V
            DbSchema::DateTime(_) => match self.0 {
                Value::String(s) => match DateTime::parse_from_rfc3339(&s) {
                    Ok(_) => Ok(Value::String(s)),
                    Err(e) => Err(ConversionError::InvalidDateTime(s, e)),
                },
                _ => Err(ConversionError::InvalidJsonValue(
                    "string",
                    self.0.value_type(),
                )),
            },
            DbSchema::String(_) => match self.0.is_string() {
                true => Ok(self.0),
                false => Err(ConversionError::InvalidJsonValue(
                    "string",
                    self.0.value_type(),
                )),
            },
            DbSchema::Integer(_) => match self.0.is_number() {
                true => Ok(Value::Number(
                    Number::from_f64(
                        self.0
                            .as_i64()
                            .or_else(|| {
                                self.0.as_f64().map(|v| f64::round(v) as i64)
                            })
                            .ok_or(ConversionError::NonIntegerNumber)?
                            as f64,
                    )
                    .ok_or(ConversionError::NonIntegerNumber)?,
                )),
                false => Err(ConversionError::InvalidJsonValue(
                    "number",
                    self.0.value_type(),
                )),
            },
            DbSchema::Double(_) => match self.0.is_number() {
                true => Ok(self.0),
                false => Err(ConversionError::InvalidJsonValue(
                    "number",
                    self.0.value_type(),
                )),
            },
            DbSchema::Bool(_) => match self.0.is_boolean() {
                true => Ok(self.0),
                false => Err(ConversionError::InvalidJsonValue(
                    "bool",
                    self.0.value_type(),
                )),
            },
            DbSchema::Json(_) => Ok(self.0),
            DbSchema::Unit(_) => match self.0.is_null() {
                true => Ok(self.0),
                false => Err(ConversionError::InvalidJsonValue(
                    "null",
                    self.0.value_type(),
                )),
            },
        }
    }

    /// Convert from in-memory representation to database representation.
    pub fn save(
        schema: &DbSchema,
        value: Value,
    ) -> Result<Self, ConversionError> {
        match schema {
            // {"attr": V} => {"attr": E}
            DbSchema::Struct(StructSchema {
                fields,
                flatten,
                elastic_rename,
                ..
            }) => {
                let mut obj = match value {
                    Value::Object(m) => Ok(m),
                    _ => Err(ConversionError::InvalidJsonValue(
                        "object",
                        value.value_type(),
                    )),
                }?;
                Ok(Self(Value::Object(fields.iter().try_fold(
                    Map::new(),
                    |mut map, (field, field_schema)| {
                        let (key, value) = match obj.remove_entry(field) {
                            Some((key, value)) => {
                                let value = Self::save(field_schema, value)
                                    .map_err(|e| e.add_path(&key))?
                                    .0;
                                (key, value)
                            }
                            None => (
                                field.to_string(),
                                field_schema.default().ok_or_else(|| {
                                    ConversionError::MissingField(
                                        field.to_string(),
                                    )
                                })?,
                            ),
                        };
                        match flatten.contains(&key) {
                            false => {
                                map.insert(
                                    elastic_rename
                                        .get(&key)
                                        .unwrap_or(&key)
                                        .to_string(),
                                    value,
                                );
                                Ok(map)
                            }
                            true => match value {
                                Value::Object(m) => {
                                    map.extend(m);
                                    Ok(map)
                                }
                                obj => Err(ConversionError::InvalidJsonValue(
                                    "object",
                                    obj.value_type(),
                                )),
                            },
                        }
                    },
                )?)))
            }
            //  {"K": V} => [{"$key": "K", "&value": EV}]
            DbSchema::Dictionary(DictionarySchema { value_type, .. }) => {
                let obj = match value {
                    Value::Object(l) => Ok(l),
                    _ => Err(ConversionError::InvalidJsonValue(
                        "object",
                        value.value_type(),
                    )),
                }?;
                Ok(Self(Value::Array(
                    obj.into_iter()
                        .map(|(key, value)| {
                            Ok(json!({
                                "key": key,
                                "value": Self::save(value_type, value).map_err(|e| e.add_path(&key))?.0
                            }))
                        })
                        .collect::<Result<_, ConversionError>>()?,
                )))
            }
            // [V] => [EV]
            DbSchema::List(ListSchema {
                value_type,
                min_length,
                max_length,
            }) => {
                let array = test_json_arr(value, *min_length, *max_length)?;
                Ok(Self(Value::Array(
                    array
                        .into_iter()
                        .enumerate()
                        .map(|(i, elem)| {
                            Ok(Self::save(value_type, elem)
                                .map_err(|e| e.add_path(i.to_string()))?
                                .0)
                        })
                        .collect::<Result<_, ConversionError>>()?,
                )))
            }
            DbSchema::Set(SetSchema {
                value_type,
                min_length,
                max_length,
            }) => {
                let array = test_json_arr(value, *min_length, *max_length)?;
                Ok(ElasticValue(Value::Array(
                    array
                        .into_iter()
                        .try_fold(Vec::new(), |mut set, elem| {
                            match set.contains(&elem) {
                                false => {
                                    set.push(elem);
                                    Ok(set)
                                }
                                true => Err(
                                    ConversionError::DuplicateValueInSet(elem),
                                ),
                            }
                        })?
                        .into_iter()
                        .enumerate()
                        .map(|(i, elem)| {
                            Ok(Self::save(value_type, elem)
                                .map_err(|e| e.add_path(i.to_string()))?
                                .0)
                        })
                        .collect::<Result<_, ConversionError>>()?,
                )))
            }
            // None => None || V => EV
            DbSchema::Option(OptionSchema { value_type, .. }) => {
                match value.is_null() {
                    true => Ok(ElasticValue(Value::Null)),
                    false => Self::save(value_type, value)
                        .map_err(|e| e.add_path("some")),
                }
            }
            // {"tag": V} => {"tag": EV}
            DbSchema::Enum(EnumSchema {
                options, format, ..
            }) => {
                let (tag, value) = match value {
                    Value::Object(map)
                        if (*format == EnumFormat::ExternallyTagged
                            || *format == EnumFormat::UnitAsString)
                            && map.len() == 1 =>
                    {
                        Ok(map.into_iter().next().unwrap())
                    }
                    Value::String(tag)
                        if *format == EnumFormat::UnitAsString
                            || *format == EnumFormat::TagString =>
                    {
                        Ok((tag, Value::Null))
                    }
                    _ => Err(ConversionError::InvalidEnumValue),
                }?;
                let option_schema = options.get(&tag).ok_or_else(|| {
                    ConversionError::MissingOption(tag.to_string())
                })?;
                let value = Self::save(option_schema, value)
                    .map_err(|e| e.add_path(&tag))?
                    .0;
                match format {
                    EnumFormat::ExternallyTagged | EnumFormat::UnitAsString => {
                        Ok(Self(Value::Object(once((tag, value)).collect())))
                    }
                    EnumFormat::TagString => Ok(Self(Value::String(tag))),
                }
            }
            DbSchema::Ipv4(_) => match value {
                Value::String(s) => match Ipv4Addr::from_str(&s) {
                    Ok(_) => Ok(Self(Value::String(s))),
                    Err(e) => Err(ConversionError::InvalidIpv4Address(s, e)),
                },
                _ => Err(ConversionError::InvalidJsonValue(
                    "string",
                    value.value_type(),
                )),
            },
            DbSchema::Ipv6(_) => match value {
                Value::String(s) => match Ipv6Addr::from_str(&s) {
                    Ok(_) => Ok(Self(Value::String(s))),
                    Err(e) => Err(ConversionError::InvalidIpv6Address(s, e)),
                },
                _ => Err(ConversionError::InvalidJsonValue(
                    "string",
                    value.value_type(),
                )),
            },
            DbSchema::DateTime(_) => match value {
                Value::String(s) => match DateTime::parse_from_rfc3339(&s) {
                    Ok(_) => Ok(Self(Value::String(s))),
                    Err(e) => Err(ConversionError::InvalidDateTime(s, e)),
                },
                _ => Err(ConversionError::InvalidJsonValue(
                    "string",
                    value.value_type(),
                )),
            },
            DbSchema::String(_) => match value.is_string() {
                true => Ok(Self(value)),
                false => Err(ConversionError::InvalidJsonValue(
                    "string",
                    value.value_type(),
                )),
            },
            DbSchema::Integer(_) => match value.is_number() {
                true => Ok(Self(Value::Number(
                    Number::from_f64(
                        value
                            .as_i64()
                            .or_else(|| {
                                value.as_f64().map(|v| f64::round(v) as i64)
                            })
                            .ok_or(ConversionError::NonIntegerNumber)?
                            as f64,
                    )
                    .ok_or(ConversionError::NonIntegerNumber)?,
                ))),
                false => Err(ConversionError::InvalidJsonValue(
                    "number",
                    value.value_type(),
                )),
            },
            DbSchema::Double(_) => match value.is_number() {
                true => Ok(Self(value)),
                false => Err(ConversionError::InvalidJsonValue(
                    "number",
                    value.value_type(),
                )),
            },
            DbSchema::Bool(_) => match value.is_boolean() {
                true => Ok(Self(value)),
                false => Err(ConversionError::InvalidJsonValue(
                    "bool",
                    value.value_type(),
                )),
            },
            DbSchema::Json(_) => Ok(Self(value)),
            DbSchema::Unit(_) => match value.is_null() {
                true => Ok(Self(value)),
                false => Err(ConversionError::InvalidJsonValue(
                    "null",
                    value.value_type(),
                )),
            },
        }
    }
}

fn load_struct(
    StructSchema {
        fields,
        flatten,
        elastic_rename,
        ..
    }: &StructSchema,
    obj: &mut Map<String, Value>,
) -> Result<Value, ConversionError> {
    Ok(Value::Object(
        fields
            .iter()
            .map(|(key, field_schema)| match flatten.contains(key) {
                false => {
                    let es_name =
                        elastic_rename.get(key).unwrap_or(key).as_str();
                    match obj.remove(es_name) {
                        Some(val) => {
                            let val = ElasticValue(val)
                                .load(field_schema)
                                .map_err(|e| e.add_path(key))?;
                            Ok((key.to_string(), val))
                        }
                        None => Ok((
                            key.to_string(),
                            field_schema.default().ok_or_else(|| {
                                ConversionError::MissingField(key.to_string())
                            })?,
                        )),
                    }
                }
                true => {
                    let val = match field_schema {
                        DbSchema::Struct(s) => load_struct(s, obj),
                        DbSchema::Enum(s)
                            if matches!(
                                s.format,
                                EnumFormat::ExternallyTagged
                                    | EnumFormat::UnitAsString
                            ) =>
                        {
                            load_flattened_enum(s, obj)
                        }
                        DbSchema::Option(s) => load_flattened_option(s, obj),
                        _ => Err(ConversionError::CannotFlatten),
                    }
                    .map_err(|e| e.add_path(key))?;
                    Ok((key.to_string(), val))
                }
            })
            .collect::<Result<_, ConversionError>>()?,
    ))
}

fn load_flattened_enum(
    EnumSchema { options, .. }: &EnumSchema,
    obj: &mut Map<String, Value>,
) -> Result<Value, ConversionError> {
    let (tag, tag_schema, value) = options
        .iter()
        .find_map(|(tag, tag_schema)| {
            let (tag, value) = obj.remove_entry(tag)?;
            Some((tag, tag_schema, value))
        })
        .ok_or(ConversionError::InvalidEnumValue)?;
    options
        .keys()
        .all(|opt| &tag == opt || !obj.contains_key(opt))
        .then_some(())
        .ok_or(ConversionError::InvalidEnumValue)?;
    let val = ElasticValue(value).load(tag_schema)?;
    Ok(Value::Object(once((tag, val)).collect()))
}

fn load_flattened_option(
    OptionSchema { value_type, .. }: &OptionSchema,
    obj: &mut Map<String, Value>,
) -> Result<Value, ConversionError> {
    match value_type.as_ref() {
        DbSchema::Struct(s) => match load_struct(s, obj) {
            Err(ConversionError::MissingField(_)) => Ok(Value::Null),
            v => v,
        },
        DbSchema::Enum(s)
            if matches!(
                s.format,
                EnumFormat::ExternallyTagged | EnumFormat::UnitAsString
            ) =>
        {
            match load_flattened_enum(s, obj) {
                Err(ConversionError::InvalidEnumValue) => Ok(Value::Null),
                v => v,
            }
        }
        _ => Err(ConversionError::CannotFlatten),
    }
}

fn test_json_arr(
    value: Value,
    min: Option<usize>,
    max: Option<usize>,
) -> Result<Vec<Value>, ConversionError> {
    match value {
        Value::Array(array) if min.map_or(false, |min| array.len() < min) => {
            Err(ConversionError::TooFewElements(min.unwrap(), array.len()))
        }
        Value::Array(array) if max.map_or(false, |max| array.len() > max) => {
            Err(ConversionError::TooManyElements(max.unwrap(), array.len()))
        }
        Value::Array(array) => Ok(array),
        _ => Err(ConversionError::InvalidJsonValue(
            "array",
            value.value_type(),
        )),
    }
}
