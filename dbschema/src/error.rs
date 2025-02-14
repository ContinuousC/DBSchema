/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::net::AddrParseError;

use serde_json::Value;
use thiserror::Error;

use super::schema::DbSchema;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("The given schema is not of the type struct: {0:?}")]
    NotAStructSchema(DbSchema),
    #[error("The given value is not a struct: {0:?}")]
    NotAStructValue(Value),
    #[error("The given value is not a list: {0:?}")]
    NotAListValue(Value),
    #[error("Unknown field: {0}")]
    UnknownField(String),
    #[error("Missing field: {0}")]
    MissingField(String),
    #[error("Missing key: {0}")]
    MissingKey(String),
    #[error("Missing option: {0}")]
    MissingOption(String),
    #[error("Deprecated option: {0}")]
    DeprecatedOption(String),
    #[error("Missing default option: {0}")]
    MissingDefaultOption(String),
    #[error("Missing deprecated option: {0}")]
    MissingDeprecatedOption(String),
    #[error("Max length is smaller than min length")]
    MaxSmallerThanMinLength,
    #[error(
        "List has too few elements: expected at least {0} elements, got {1}"
    )]
    ListTooShort(usize, usize),
    #[error(
        "List has too many elements: expected at most {0} elements, got {1}"
    )]
    ListTooLong(usize, usize),
    #[error("Unhashable value for list with unique elements")]
    UnhashableValue,
    #[error("Value type error: expected {0}, got {1}")]
    ValueTypeError(&'static str, &'static str),
    #[error("Invalid enum value")]
    InvalidEnumValue,
    #[error("Invalid ipv4 address '{0}': {1}")]
    InvalidIpv4Addr(String, AddrParseError),
    #[error("Invalid ipv6 address '{0}': {1}")]
    InvalidIpv6Addr(String, AddrParseError),
    #[error("Invalid datetime '{0}': {1}")]
    InvalidDateTime(String, chrono::ParseError),
    #[error("Values of type {0} are not orderable")]
    InvalidComparison(&'static str),
    #[error("Number is not an integer")]
    NumberNotInteger,
    #[error("Number is not floating-point")]
    NumberNotDouble,
    #[error("Invalid path in schema: {0}")]
    InvalidPath(super::FilterPath),
    #[error("Value is not iterable: {0}")]
    NotIterable(&'static str),
    #[error("Cannot flatten field '{0}' since it is not a struct")]
    CannotFlatten(String),
    #[error("Missing field '{0}' listed in 'flatten'")]
    MissingFlattenField(String),
    #[error("Non-unit option in 'tag_string' enum: {0}")]
    NonUnitOptionInTagString(String),
    #[error("Failed to serialize value: {0}")]
    Serialize(serde_json::Error),
    #[error("{0} is not backward compatible with {1}")]
    IncompatibleKinds(&'static str, &'static str),
    #[error("adding min_length is not backwards compatible")]
    AddedMinLength,
    #[error("adding max_length is not backwards compatible")]
    AddedMaxLength,
    #[error("min_length {0} is larger than min_length {1}")]
    IncreasedMinLength(usize, usize),
    #[error("max_length {0} is smaller than max_length {1}")]
    DecreasedMaxLength(usize, usize),
    #[error("added fields without default: {}", .0.join(", "))]
    AddedFieldsWithoutDefault(Vec<String>),
    #[error("unlisted removed fields: {}", .0.join(", "))]
    UnlistedRemovedFields(Vec<String>),
    #[error("\"unremoved\" fields: {}", .0.join(", "))]
    UnremovedFields(Vec<String>),
    #[error("reused previously removed fields: {}", .0.join(", "))]
    ReaddedFields(Vec<String>),
    #[error("removed options: {}", .0.join(", "))]
    RemovedOptions(Vec<String>),
    #[error("invalid enum format transition")]
    InvalidEnumFormatTransition,
    #[error(
        "invalid metrics for {0}, table {1} \
		 (should contain exactly one result row)"
    )]
    InvalidMetrics(String, String),
    #[error("invalid default value")]
    InvalidDefaultValue,
    #[error("{}: {1}", .0.join("."))]
    Path(Vec<String>, Box<Error>),
}

impl Error {
    pub(crate) fn add_path<T: Into<String>>(self, elem: T) -> Self {
        match self {
            Self::Path(mut elems, err) => {
                elems.insert(0, elem.into());
                Self::Path(elems, err)
            }
            err => Self::Path(vec![elem.into()], Box::new(err)),
        }
    }
}
