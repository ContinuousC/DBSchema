/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::cmp::Ordering;

use serde_json::Value;

use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::SetSchema;

use super::error::{Error, Result};
use super::filter_path::FilterPath;
use super::schema::{DbSchema, DictionarySchema, ListSchema};
use super::value_ext::ValueExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    All(Vec<Filter>),
    Any(Vec<Filter>),
    Not(Box<Filter>),
    At(At),
    AllElems(Box<Filter>),
    AnyElem(Box<Filter>),
    Eq(Value),
    Ne(Value),
    In(Vec<Value>),
    NotIn(Vec<Value>),
    Gt(Value),
    Ge(Value),
    Le(Value),
    Lt(Value),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct At {
    pub path: FilterPath,
    pub filter: Box<Filter>,
}

impl Filter {
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::All(mut xs), Self::All(ys)) => {
                xs.extend(ys);
                Self::All(xs)
            }
            (Self::All(mut xs), y) => {
                xs.push(y);
                Self::All(xs)
            }
            (x, Self::All(mut ys)) => {
                ys.insert(0, x);
                Self::All(ys)
            }
            (x, y) => Self::All(vec![x, y]),
        }
    }

    pub fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::Any(mut xs), Self::Any(ys)) => {
                xs.extend(ys);
                Self::Any(xs)
            }
            (Self::Any(mut xs), y) => {
                xs.push(y);
                Self::Any(xs)
            }
            (x, Self::Any(mut ys)) => {
                ys.insert(0, x);
                Self::Any(ys)
            }
            (x, y) => Self::Any(vec![x, y]),
        }
    }

    pub fn negate(filter: Filter) -> Self {
        Self::Not(Box::new(filter))
    }

    pub fn at(path: FilterPath, filter: Filter) -> Self {
        Self::At(At {
            path,
            filter: Box::new(filter),
        })
    }

    pub fn all_elems(filter: Filter) -> Self {
        Self::AllElems(Box::new(filter))
    }

    pub fn any_elem(filter: Filter) -> Self {
        Self::AnyElem(Box::new(filter))
    }

    pub fn verify(&self, schema: &DbSchema) -> Result<()> {
        match self {
            Filter::All(filters) | Filter::Any(filters) => {
                filters.iter().try_for_each(|filter| filter.verify(schema))
            }
            Filter::Not(filter) => filter.verify(schema),
            Filter::At(At { path, filter }) => {
                filter.verify(path.walk_schema(schema)?)
            }
            Filter::AllElems(filter) | Filter::AnyElem(filter) => {
                match schema {
                    DbSchema::Dictionary(DictionarySchema {
                        value_type,
                        ..
                    })
                    | DbSchema::List(ListSchema { value_type, .. }) => {
                        filter.verify(value_type)
                    }
                    DbSchema::Set(SetSchema { value_type, .. }) => {
                        filter.verify(value_type)
                    }
                    _ => Err(Error::NotIterable(schema.kind())),
                }
            }
            Filter::Eq(value) | Filter::Ne(value) => schema.verify_value(value),
            Filter::In(values) | Filter::NotIn(values) => {
                values.iter().try_for_each(|v| schema.verify_value(v))
            }
            Filter::Gt(value)
            | Filter::Ge(value)
            | Filter::Le(value)
            | Filter::Lt(value) => {
                match schema {
                    DbSchema::Double(_)
                    | DbSchema::Integer(_)
                    | DbSchema::DateTime(_) => Ok(()),
                    _ => Err(Error::InvalidComparison(schema.kind())),
                }?;
                schema.verify_value(value)
            }
        }
    }

    pub fn matches(&self, schema: &DbSchema, value: &Value) -> Result<bool> {
        match self {
            Filter::All(filters) => {
                filters.iter().try_fold(true, |res, filter| {
                    Ok(res && filter.matches(schema, value)?)
                })
            }
            Filter::Any(filters) => {
                filters.iter().try_fold(false, |res, filter| {
                    Ok(res || filter.matches(schema, value)?)
                })
            }
            Filter::Not(filter) => Ok(!filter.matches(schema, value)?),
            Filter::At(At { path, filter }) => {
                match path.walk_value(schema, value)? {
                    (schema, Some(value)) => filter.matches(schema, value),
                    (_schema, None) => Ok(false),
                }
            }
            Filter::AllElems(filter) => match schema {
                DbSchema::Dictionary(DictionarySchema {
                    value_type, ..
                }) => value
                    .as_object_or_err()?
                    .values()
                    .try_fold(true, |res, elem| {
                        Ok(res && filter.matches(value_type, elem)?)
                    }),
                DbSchema::List(ListSchema { value_type, .. })
                | DbSchema::Set(SetSchema { value_type, .. }) => value
                    .as_array_or_err()?
                    .iter()
                    .try_fold(true, |res, elem| {
                        Ok(res && filter.matches(value_type, elem)?)
                    }),
                _ => Err(Error::NotIterable(schema.kind())),
            },
            Filter::AnyElem(filter) => match schema {
                DbSchema::Dictionary(DictionarySchema {
                    value_type, ..
                }) => value
                    .as_object_or_err()?
                    .values()
                    .try_fold(false, |res, elem| {
                        Ok(res || filter.matches(value_type, elem)?)
                    }),
                DbSchema::List(ListSchema { value_type, .. })
                | DbSchema::Set(SetSchema { value_type, .. }) => value
                    .as_array_or_err()?
                    .iter()
                    .try_fold(false, |res, elem| {
                        Ok(res || filter.matches(value_type, elem)?)
                    }),
                _ => Err(Error::NotIterable(schema.kind())),
            },
            Filter::Eq(cmp) => Ok(value == cmp),
            Filter::Ne(cmp) => Ok(value != cmp),
            Filter::In(cmp) => Ok(cmp.contains(value)),
            Filter::NotIn(cmp) => Ok(!cmp.contains(value)),
            Filter::Gt(cmp) => Ok(compare(schema, value, cmp)?.is_gt()),
            Filter::Ge(cmp) => Ok(compare(schema, value, cmp)?.is_ge()),
            Filter::Le(cmp) => Ok(compare(schema, value, cmp)?.is_le()),
            Filter::Lt(cmp) => Ok(compare(schema, value, cmp)?.is_lt()),
        }
    }
}

fn compare(schema: &DbSchema, a: &Value, b: &Value) -> Result<Ordering> {
    match schema {
        DbSchema::Double(_) => {
            Ok(a.as_f64_or_err()?
                .partial_cmp(&b.as_f64_or_err()?)
                .unwrap_or(
                    /* should never happen since NaN and friends are disallowed in json */
                    Ordering::Equal,
                ))
        }
        DbSchema::Integer(_) => Ok(a.as_i64_or_err()?.cmp(&b.as_i64_or_err()?)),
        DbSchema::DateTime(_) => {
            let a = a.as_str_or_err()?;
            let b = b.as_str_or_err()?;
            Ok(DateTime::parse_from_rfc3339(a)
                .map_err(|e| Error::InvalidDateTime(a.to_string(), e))?
                .cmp(
                    &DateTime::parse_from_rfc3339(b).map_err(|e| {
                        Error::InvalidDateTime(b.to_string(), e)
                    })?,
                ))
        }
        _ => Err(Error::InvalidComparison(schema.kind())),
    }
}
