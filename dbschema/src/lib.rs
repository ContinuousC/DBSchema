/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

mod error;
mod filter;
mod filter_path;
mod schema;
mod table;
mod tagged;
mod value_ext;
mod versioning;

pub use dbschema_derive::HasSchema;

pub use error::{Error, Result};
pub use filter::{At, Filter};
pub use filter_path::{FilterPath, PathElem};
pub use schema::{
    BoolSchema, Compatibility, DateTimeSchema, DbSchema, DictionarySchema,
    DoubleSchema, EnumFormat, EnumSchema, HasSchema, HasSchema1, HasSchema2,
    HasSchema3, HasSchema4, IntegerSchema, Ipv4Schema, Ipv6Schema, JsonSchema,
    ListSchema, OptionSchema, SetSchema, StringSchema, StructSchema,
    UnitSchema,
};
pub use table::{DbTable, DbTableId, HasTableDef};
pub use tagged::Tagged;
pub use value_ext::ValueExt;
pub use versioning::{
    Anchor, DualVersionInfo, DualVersioned, DualVersionedValue, Identified,
    ObjectId, SingleVersionInfo, SingleVersioned, SingleVersionedValue,
    TimeRange, Timeline, Timestamped, TimestampedValue, Versioned,
    VersioningType,
};
