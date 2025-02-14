/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::value_ext::ValueExt;

use super::error::{Error, Result};
use super::filter::{At, Filter};
use super::schema::{DbSchema, EnumSchema, OptionSchema, StructSchema};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(transparent)]
pub struct FilterPath(pub Vec<PathElem>);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PathElem {
    /// Descend into struct field
    Field(String),
    /// Check enum tag and descend
    Option(String),
    /// Check option and descend
    Some,
}

impl FilterPath {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field<T: Into<String>>(mut self, field: T) -> Self {
        self.0.push(PathElem::Field(field.into()));
        self
    }

    pub fn option<T: Into<String>>(mut self, option: T) -> Self {
        self.0.push(PathElem::Option(option.into()));
        self
    }

    pub fn some(mut self) -> Self {
        self.0.push(PathElem::Some);
        self
    }

    pub fn filter(self, filter: Filter) -> Filter {
        Filter::At(At {
            path: self,
            filter: Box::new(filter),
        })
    }

    pub fn eq(self, val: Value) -> Filter {
        self.filter(Filter::Eq(val))
    }

    pub fn ne(self, val: Value) -> Filter {
        self.filter(Filter::Ne(val))
    }

    pub fn eq_any(self, vals: Vec<Value>) -> Filter {
        self.filter(Filter::In(vals))
    }

    pub fn ne_any(self, vals: Vec<Value>) -> Filter {
        self.filter(Filter::NotIn(vals))
    }

    pub fn lt(self, val: Value) -> Filter {
        self.filter(Filter::Lt(val))
    }

    pub fn le(self, val: Value) -> Filter {
        self.filter(Filter::Le(val))
    }

    pub fn ge(self, val: Value) -> Filter {
        self.filter(Filter::Ge(val))
    }

    pub fn gt(self, val: Value) -> Filter {
        self.filter(Filter::Gt(val))
    }

    pub fn walk_schema<'a>(
        &self,
        schema: &'a DbSchema,
    ) -> Result<&'a DbSchema> {
        let err = || Error::InvalidPath(self.clone());
        self.0.iter().try_fold(schema, |schema, elem| match elem {
            PathElem::Field(field) => match schema {
                DbSchema::Struct(StructSchema { fields, .. }) => {
                    fields.get(field).ok_or_else(err)
                }
                _ => Err(err()),
            },
            PathElem::Option(option) => match schema {
                DbSchema::Enum(EnumSchema { options, .. }) => {
                    options.get(option).ok_or_else(err)
                }
                _ => Err(err()),
            },
            PathElem::Some => match schema {
                DbSchema::Option(OptionSchema { value_type, .. }) => {
                    Ok(&**value_type)
                }
                _ => Err(err()),
            },
        })
    }

    pub fn walk_value<'a, 'b>(
        &self,
        schema: &'a DbSchema,
        value: &'b Value,
    ) -> Result<(&'a DbSchema, Option<&'b Value>)> {
        let err = || Error::InvalidPath(self.clone());
        self.0.iter().try_fold(
            (schema, Some(value)),
            |(schema, value), elem| match elem {
                PathElem::Field(field) => match schema {
                    DbSchema::Struct(StructSchema { fields, .. }) => Ok((
                        fields.get(field).ok_or_else(err)?,
                        value.map_or(Ok(None), |value| {
                            Ok(value.as_object_or_err()?.get(field))
                        })?,
                    )),
                    _ => Err(err()),
                },
                PathElem::Option(option) => match schema {
                    DbSchema::Enum(EnumSchema {
                        options, format, ..
                    }) => Ok((
                        options.get(option).ok_or_else(err)?,
                        value.map_or(Ok(None), |value| {
                            let (tag, value) = value.as_enum_or_err(format)?;
                            Ok((tag == option).then_some(value))
                        })?,
                    )),
                    _ => Err(err()),
                },
                PathElem::Some => match schema {
                    DbSchema::Option(OptionSchema { value_type, .. }) => Ok((
                        &**value_type,
                        value.and_then(|value| {
                            (!value.is_null()).then_some(value)
                        }),
                    )),
                    _ => Err(err()),
                },
            },
        )
    }
}

impl fmt::Display for FilterPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, elem) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, " :: ")?;
                write!(f, "{elem}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for PathElem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathElem::Field(field) => write!(f, "field '{field}'"),
            PathElem::Option(option) => write!(f, "option '{option}'"),
            PathElem::Some => write!(f, "some"),
        }
    }
}
