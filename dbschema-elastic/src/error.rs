/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use serde_json::Value;
use thiserror::Error;

use dbschema::{DbSchema, Filter};

#[derive(Error, Debug)]
pub enum FilterError {
    #[error("Not (yet) implemented: {0:?}")]
    NotImplemented(Filter),
    #[error("{0} is not comparable")]
    NotComparable(&'static str),
    #[error("{0} is not valid for in / not_in filter")]
    NotInComparable(&'static str),
    #[error("{0} is not orderable")]
    NotOrderable(&'static str),
    #[error("{0} is not iterable")]
    NotIterable(&'static str),
    #[error("Encountered a problem with the id: {0}")]
    IdError(String),
    #[error("Unable to Serialize the operator: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Invalid value in filter")]
    InvalidValue,
    #[error("Missing field: {0}")]
    MissingField(String),
    #[error("Missing option: {0}")]
    MissingOption(String),
    #[error("Schema error: {0}")]
    Schema(#[from] dbschema::Error),
    #[error("Invalid schema: {1}; expected: {0}")]
    InvalidSchema(&'static str, &'static str),
}

#[derive(Error, Debug)]
pub enum MappingError {
    #[error("excpected: {0}, got: {1:?}")]
    UnexpectedValue(String, Value),
    #[error("Not yet implemented: {0:?}")]
    NotImplemented(DbSchema),
    #[error("Only structs and externally tagged enums can be flattened")]
    FlattenNonStruct,
    #[error("Internal error: {0}")]
    Internal(&'static str),
}

#[derive(Error, Debug, Clone)]
pub enum ConversionError {
    #[error("{1} at '{}'", .0.join("."))]
    Path(Vec<String>, Box<ConversionError>),
    #[error("Invalid json value: expected {0}, got {1}")]
    InvalidJsonValue(&'static str, &'static str),
    #[error("Invalid enum value")]
    InvalidEnumValue,
    #[error("Only structs and externally tagged enums can be flattened")]
    CannotFlatten,
    #[error("Failed to load flattened enum")]
    LoadFlattenedEnum,
    #[error("Duplicate value in set: {}", serde_json::to_string(.0).as_deref().unwrap_or("(json error)"))]
    DuplicateValueInSet(Value),
    #[error(
        "Jsonvalue does not comply with database schema: expected a {0:?}, got {1} (reason: {2}). path: {3:?}"
    )]
    JsonDoesNotComply(DbSchema, Value, String, Vec<String>),
    #[error("Schema error in {0:?}: {1}")]
    Schema(DbSchema, String),
    #[error("Excpected a dictobject, but recieved: {0:?}")]
    NoDictObj(Value),
    #[error("Excpected a struct, but recieved: {0:?}")]
    NoStruct(Value),
    #[error("Missing field {0}, which does not have a default")]
    MissingField(String),
    #[error("Unknown option {0}")]
    MissingOption(String),
    #[error("Not yet implemented: {0:?}")]
    NotImplemented(DbSchema),
    #[error("Too few elements: {1} < {0}")]
    TooFewElements(usize, usize),
    #[error("Too many elements: {1} > {0}")]
    TooManyElements(usize, usize),
    #[error("Invalid ipv4 address '{0}': {1}")]
    InvalidIpv4Address(String, std::net::AddrParseError),
    #[error("Invalid ipv6 address '{0}': {1}")]
    InvalidIpv6Address(String, std::net::AddrParseError),
    #[error("Invalid date_time value '{0}': {1}")]
    InvalidDateTime(String, chrono::ParseError),
    #[error("Expected an integral number")]
    NonIntegerNumber,
}

impl ConversionError {
    pub(crate) fn add_path<T: ToString>(self, elem: T) -> Self {
        match self {
            Self::Path(mut path, err) => {
                path.push(elem.to_string());
                Self::Path(path, err)
            }
            _ => Self::Path(vec![elem.to_string()], Box::new(self)),
        }
    }
}
