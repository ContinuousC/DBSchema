/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, map::Map, Value};

use crate::table::NamedFieldSchema;

use super::error::{Error, Result};
use super::value_ext::ValueExt;

/// The type has a schema. Laws:
/// - If a Serialize impl exists, it should serialize to a valid
///   value for the schema.
/// - If a Deserialize impl exists, it should deserialize any valid
///   value for the schema.
pub trait HasSchema {
    fn schema() -> DbSchema;
}

/// The type has a schema with one dynamic schema param
/// (e.g. an internal serde_json::Value).
pub trait HasSchema1 {
    fn schema1(value: DbSchema) -> DbSchema;
}

/// The type has a schema with two dynamic schema params
/// (e.g. internal serde_json::Values).
pub trait HasSchema2 {
    fn schema2(value1: DbSchema, value2: DbSchema) -> DbSchema;
}

/// The type has a schema with three dynamic schema params
/// (e.g. internal serde_json::Values).
pub trait HasSchema3 {
    fn schema3(
        value1: DbSchema,
        value2: DbSchema,
        value3: DbSchema,
    ) -> DbSchema;
}

/// The type has a schema with four dynamic schema params
/// (e.g. internal serde_json::Values).
pub trait HasSchema4 {
    fn schema4(
        value1: DbSchema,
        value2: DbSchema,
        value3: DbSchema,
        value4: DbSchema,
    ) -> DbSchema;
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DbSchema {
    // param: primary_key, {fields: DbSchema}
    Struct(StructSchema),
    // valuetype
    Dictionary(DictionarySchema),
    // param: type contained in the list, min and max amount of values
    // constraint for elastic: no lists of lists
    List(ListSchema),
    Set(SetSchema),
    // param: type present if option is not None
    Option(OptionSchema),
    // param: map of all options (tag, value),
    Enum(EnumSchema),
    // Generic JSON type
    Json(JsonSchema),
    Ipv4(Ipv4Schema),
    Ipv6(Ipv6Schema),
    // format is iso-8601
    // requires string as jsonvalue
    DateTime(DateTimeSchema),
    // LocalDateTime, to be discussed
    String(StringSchema),
    Integer(IntegerSchema),
    Double(DoubleSchema),
    Bool(BoolSchema),
    // equivalent of rust (())
    Unit(UnitSchema),
}

#[derive(Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Debug)]
pub enum Compatibility {
    Compatible,
    NeedsReindex,
}

impl DbSchema {
    pub fn get_pks(&self, value: &Value) -> Result<BTreeMap<String, Value>> {
        // prepare variables
        let struct_schema = if let DbSchema::Struct(struct_schema) = self {
            struct_schema
        } else {
            return Err(Error::NotAStructSchema(self.clone()));
        };
        let value = if let Value::Object(map) = value {
            map
        } else {
            return Err(Error::NotAStructValue(value.clone()));
        };

        let mut pks: BTreeMap<String, Value> = BTreeMap::new();
        for pk in &struct_schema.pk {
            pks.insert(
                pk.clone(),
                value
                    .get(pk)
                    .ok_or_else(|| Error::MissingField(pk.clone()))?
                    .clone(),
            );
        }
        Ok(pks)
    }

    /// Provide a description for the schema variant, for error handling.
    pub fn kind(&self) -> &'static str {
        match self {
            DbSchema::Struct(_) => "struct",
            DbSchema::Dictionary(_) => "dictionary",
            DbSchema::List(_) => "list",
            DbSchema::Set(_) => "set",
            DbSchema::Option(_) => "option",
            DbSchema::Enum(_) => "enum",
            DbSchema::Json(_) => "json",
            DbSchema::Ipv4(_) => "ipv4",
            DbSchema::Ipv6(_) => "ipv6",
            DbSchema::DateTime(_) => "date_time",
            DbSchema::String(_) => "string",
            DbSchema::Integer(_) => "integer",
            DbSchema::Double(_) => "double",
            DbSchema::Bool(_) => "bool",
            DbSchema::Unit(_) => "unit",
        }
    }

    #[deprecated = "use DbSchema::into_named_field_schema"]
    pub fn into_struct(self) -> Option<NamedFieldSchema> {
        self.into_named_field_schema()
    }

    pub fn into_named_field_schema(self) -> Option<NamedFieldSchema> {
        match self {
            Self::Struct(s) => Some(NamedFieldSchema::Struct(s)),
            Self::Enum(s)
                if matches!(s.format, EnumFormat::ExternallyTagged) =>
            {
                Some(NamedFieldSchema::Enum(s))
            }
            _ => None,
        }
    }

    pub fn value_eq(&self, a: &Value, b: &Value) -> Result<bool> {
        match self {
            DbSchema::Struct(s) => {
                let a = a.as_object_or_err()?;
                let b = b.as_object_or_err()?;
                s.fields.iter().try_fold(true, |r, (field, schema)| {
                    let default = match schema {
                        DbSchema::Option(_) => Some(Value::Null),
                        _ => None,
                    };
                    Ok(r && schema.value_eq(
                        a.get(field).or(default.as_ref()).ok_or_else(|| {
                            Error::MissingField(field.to_string())
                        })?,
                        b.get(field).or(default.as_ref()).ok_or_else(|| {
                            Error::MissingField(field.to_string())
                        })?,
                    )?)
                })
            }
            DbSchema::Dictionary(s) => {
                let a = a.as_object_or_err()?;
                let b = b.as_object_or_err()?;
                Ok(a.iter().try_fold(true, |r, (key, a)| {
                    Ok(r && b
                        .get(key)
                        .map_or(Ok(false), |b| s.value_type.value_eq(a, b))?)
                })? && b.keys().all(|key| a.contains_key(key)))
            }
            DbSchema::List(s) => {
                let a = a.as_array_or_err()?;
                let b = b.as_array_or_err()?;
                Ok(a.len() == b.len()
                    && a.iter().zip(b).try_fold(true, |r, (a, b)| {
                        Ok(r && s.value_type.value_eq(a, b)?)
                    })?)
            }
            DbSchema::Set(s) => {
                let a = a.as_array_or_err()?;
                let b = b.as_array_or_err()?;

                match a.len() == b.len() {
                    true => {
                        /* TODO: define and exploit hashability property of
                         * set elements? */

                        let mut b = b.iter().collect::<Vec<_>>();
                        a.iter().try_fold(true, |r, a| {
                            Ok(r && b
                                .iter()
                                .enumerate()
                                .find_map(|(i, b)| {
                                    s.value_type
                                        .value_eq(a, b)
                                        .map(|r| r.then_some(i))
                                        .transpose()
                                })
                                .transpose()?
                                .map_or(false, |i| {
                                    b.swap_remove(i);
                                    true
                                }))
                        })
                    }
                    false => Ok(false),
                }
            }
            DbSchema::Option(s) => match (a, b) {
                (Value::Null, Value::Null) => Ok(true),
                (Value::Null, _) | (_, Value::Null) => Ok(false),
                _ => s.value_type.value_eq(a, b),
            },
            DbSchema::Enum(s) => {
                let (ak, av) = a.as_enum_or_err(&s.format)?;
                let (bk, bv) = b.as_enum_or_err(&s.format)?;
                let s = s
                    .options
                    .get(ak)
                    .ok_or_else(|| Error::MissingOption(ak.to_string()))?;
                Ok(ak == bk && s.value_eq(av, bv)?)
            }
            DbSchema::Json(_) => Ok(a == b),
            DbSchema::Ipv4(_) => {
                let a = a.as_str_or_err()?;
                let a = Ipv4Addr::from_str(a)
                    .map_err(|e| Error::InvalidIpv4Addr(a.to_string(), e))?;
                let b = b.as_str_or_err()?;
                let b = Ipv4Addr::from_str(b)
                    .map_err(|e| Error::InvalidIpv4Addr(b.to_string(), e))?;
                Ok(a == b)
            }
            DbSchema::Ipv6(_) => {
                let a = a.as_str_or_err()?;
                let a = Ipv6Addr::from_str(a)
                    .map_err(|e| Error::InvalidIpv6Addr(a.to_string(), e))?;
                let b = b.as_str_or_err()?;
                let b = Ipv6Addr::from_str(b)
                    .map_err(|e| Error::InvalidIpv6Addr(b.to_string(), e))?;
                Ok(a == b)
            }
            DbSchema::DateTime(_) => {
                let a = a.as_str_or_err()?;
                let a = DateTime::parse_from_rfc3339(a)
                    .map_err(|e| Error::InvalidDateTime(a.to_string(), e))?;
                let b = b.as_str_or_err()?;
                let b = DateTime::parse_from_rfc3339(b)
                    .map_err(|e| Error::InvalidDateTime(b.to_string(), e))?;
                Ok(a == b)
            }
            DbSchema::String(_) => {
                let a = a.as_str_or_err()?;
                let b = b.as_str_or_err()?;
                Ok(a == b)
            }
            DbSchema::Integer(_) => {
                let a = a.as_i64_or_err()?;
                let b = b.as_i64_or_err()?;
                Ok(a == b)
            }
            DbSchema::Double(_) => {
                let a = a.as_f64_or_err()?;
                let b = b.as_f64_or_err()?;
                Ok(a == b)
            }
            DbSchema::Bool(_) => {
                let a = a.as_bool_or_err()?;
                let b = b.as_bool_or_err()?;
                Ok(a == b)
            }
            DbSchema::Unit(_) => {
                a.as_null_or_err()?;
                b.as_null_or_err()?;
                Ok(true)
            }
        }
    }

    pub fn diff(
        &self,
        a: &Value,
        b: &Value,
    ) -> Result<Vec<(Vec<String>, String)>> {
        let mut differences: Vec<(Vec<String>, String)> = Vec::new();

        match self {
            Self::Struct(StructSchema { fields, .. }) => {
                for (k, v) in fields {
                    for (mut ps, msg) in v
                        .diff(a.get(k).unwrap(), b.get(k).unwrap())?
                        .into_iter()
                    {
                        let mut vec = vec![k.clone()];
                        vec.append(&mut ps);
                        differences.push((vec, msg));
                    }
                }
            }
            Self::Dictionary(DictionarySchema { value_type }) => {
                let a_obj = a
                    .as_object()
                    .ok_or_else(|| Error::NotAStructValue(a.clone()))?;
                let a_keys: HashSet<String> =
                    a_obj.keys().map(|s| s.to_string()).collect();
                let b_obj = b
                    .as_object()
                    .ok_or_else(|| Error::NotAStructValue(b.clone()))?;
                let b_keys: HashSet<String> =
                    b_obj.keys().map(|s| s.to_string()).collect();

                // added
                for k in b_keys.difference(&a_keys) {
                    differences.push((
                        Vec::new(),
                        format!("added {}, {}", &k, b_obj.get(k).unwrap()),
                    ));
                }
                // removed
                for k in a_keys.difference(&b_keys) {
                    differences.push((
                        Vec::new(),
                        format!("removed {}, {}", &k, a_obj.get(k).unwrap()),
                    ));
                }
                for k in a_keys.intersection(&b_keys) {
                    for (mut ps, msg) in value_type
                        .diff(a.get(k).unwrap(), b.get(k).unwrap())?
                        .into_iter()
                    {
                        let mut vec = vec![k.clone()];
                        vec.append(&mut ps);
                        differences.push((vec, msg));
                    }
                }
            }
            Self::List(ListSchema { value_type, .. }) => {
                let a_arr = a
                    .as_array()
                    .ok_or_else(|| Error::NotAListValue(a.clone()))?;
                let b_arr = b
                    .as_array()
                    .ok_or_else(|| Error::NotAListValue(b.clone()))?;
                let len_diff = a_arr.len() as isize - b_arr.len() as isize;
                if len_diff != 0 {
                    differences.push((
                        Vec::new(),
                        format!(
                            "{} items {}",
                            len_diff.abs(),
                            if len_diff < 0 { "removed" } else { "added" }
                        ),
                    ));
                }
                for (idx, (av, bv)) in a_arr.iter().zip(b_arr).enumerate() {
                    for (mut ps, msg) in value_type.diff(av, bv)? {
                        let mut vec = vec![idx.to_string()];
                        vec.append(&mut ps);
                        differences.push((vec, msg));
                    }
                }
            }
            Self::Option(OptionSchema { value_type, .. }) => {
                if a.is_null() && !b.is_null() {
                    differences.push((
                        Vec::new(),
                        format!("changed from unset to {b:?}"),
                    ));
                } else if a.is_null() && !b.is_null() {
                    differences.push((
                        Vec::new(),
                        format!("changed from {a:?} to unset"),
                    ));
                } else if !a.is_null() && !b.is_null() {
                    differences.append(&mut value_type.diff(a, b)?);
                }
            }
            Self::Enum(EnumSchema { options, .. }) => {
                let ak = a
                    .as_object()
                    .ok_or_else(|| Error::NotAStructValue(a.clone()))?
                    .keys()
                    .next()
                    .ok_or_else(|| {
                        Error::MissingField(String::from("enum tag"))
                    })?;
                let bk = b
                    .as_object()
                    .ok_or_else(|| Error::NotAStructValue(b.clone()))?
                    .keys()
                    .next()
                    .ok_or_else(|| {
                        Error::MissingField(String::from("enum tag"))
                    })?;
                if ak != bk {
                    differences.push((
                        Vec::new(),
                        format!("changed from {ak:?} to {bk:?}"),
                    ));
                } else {
                    for (mut ps, msg) in options
                        .get(&ak.to_string())
                        .unwrap()
                        .diff(a.get(ak).unwrap(), b.get(bk).unwrap())?
                        .into_iter()
                    {
                        let mut vec = vec![ak.clone()];
                        vec.append(&mut ps);
                        differences.push((vec, msg));
                    }
                }
            }
            _ => {
                if a != b {
                    differences.push((
                        Vec::new(),
                        format!("changed from {a} to {b}",),
                    ));
                }
            }
        }

        Ok(differences)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct StructSchema {
    /// Fields that are part of the primary key (for databases
    /// that support such notion).
    #[serde(default)]
    pub pk: Vec<String>,
    /// Field schema definitions.
    pub fields: BTreeMap<String, DbSchema>,
    /// Field names that can no longer be used.
    #[serde(default)]
    pub removed_fields: BTreeSet<String>,
    /// Fields to flatten in db representation (schema must be struct)
    #[serde(default)]
    pub flatten: BTreeSet<String>,
    /// Fields to rename in elasticsearch db representation(schema must be struct)
    #[serde(default)]
    pub elastic_rename: BTreeMap<String, String>,
}

impl From<StructSchema> for DbSchema {
    fn from(schema: StructSchema) -> Self {
        DbSchema::Struct(schema)
    }
}

impl StructSchema {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn field<T: Into<String>, U: Into<DbSchema>>(
        mut self,
        name: T,
        schema: U,
    ) -> Self {
        self.fields.insert(name.into(), schema.into());
        self
    }
    pub fn flatten<T: Into<String>>(mut self, key: T) -> Self {
        self.flatten.insert(key.into());
        self
    }
    pub fn serde_flatten<T: Into<String>>(mut self, key: T) -> Result<Self> {
        let key = key.into();
        let (s, opt) = match self.fields.remove(&key) {
            Some(DbSchema::Struct(s)) => (s, false),
            Some(DbSchema::Option(os)) => match *os.value_type {
                DbSchema::Struct(s) => (s, true),
                _ => return Err(Error::CannotFlatten(key.clone())),
            },
            _ => return Err(Error::CannotFlatten(key.clone())),
        };

        self.flatten.remove(&key);
        self.elastic_rename.remove(&key);

        if opt {
            self.fields
                .extend(s.fields.into_iter().map(|(name, schema)| {
                    (
                        name,
                        match &schema {
                            DbSchema::Option(_) => schema,
                            _ => OptionSchema::new(schema).into(),
                        },
                    )
                }));
        } else {
            self.fields.extend(s.fields);
        }
        self.flatten.extend(s.flatten);
        self.elastic_rename.extend(s.elastic_rename);
        self.removed_fields.extend(s.removed_fields);

        self.pk = self
            .pk
            .into_iter()
            .filter(|k| k != &key)
            .chain(s.pk)
            .collect();

        Ok(self)
    }
    pub fn elastic_rename<S: Into<String>, T: Into<String>>(
        mut self,
        key: S,
        name: T,
    ) -> Self {
        self.elastic_rename.insert(key.into(), name.into());
        self
    }
    pub fn primary_key<T: Into<String>>(mut self, key: T) -> Self {
        self.pk.push(key.into());
        self
    }
    pub fn removed<T: Into<String>>(mut self, key: T) -> Self {
        self.removed_fields.insert(key.into());
        self
    }
    pub fn set_default(mut self, mut value: Value) -> Result<Self> {
        let field_defaults =
            value.as_object_mut().ok_or(Error::InvalidDefaultValue)?;
        self.fields = self
            .fields
            .into_iter()
            .map(move |(name, schema)| {
                let default = field_defaults
                    .remove(&name)
                    .or_else(|| schema.default())
                    .ok_or(Error::InvalidDefaultValue)?;
                Ok((name, schema.set_default(default)?))
            })
            .collect::<Result<_>>()?;
        Ok(self)
    }

    fn flattened(&self) -> Result<Self> {
        let flattened_fields = self
            .flatten
            .iter()
            .map(|field| match self.fields.get(field) {
                Some(DbSchema::Struct(s)) => s.flattened(),
                Some(DbSchema::Enum(s)) => s
                    .flattened()
                    .ok_or_else(|| Error::CannotFlatten(field.to_string())),
                _ => Err(Error::CannotFlatten(field.to_string())),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StructSchema {
            fields: self
                .fields
                .iter()
                .filter(|(k, _)| !self.flatten.contains(*k))
                .chain(flattened_fields.iter().flat_map(|s| s.fields.iter()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            removed_fields: self
                .removed_fields
                .iter()
                .chain(
                    flattened_fields
                        .iter()
                        .flat_map(|s| s.removed_fields.iter()),
                )
                .cloned()
                .collect(),
            flatten: BTreeSet::new(),
            elastic_rename: self
                .elastic_rename
                .iter()
                .filter(|(k, _)| !self.flatten.contains(*k))
                .chain(
                    flattened_fields
                        .iter()
                        .flat_map(|s| s.elastic_rename.iter()),
                )
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            pk: self
                .pk
                .iter()
                .filter(|k| !self.flatten.contains(*k))
                .chain(flattened_fields.iter().flat_map(|s| s.pk.iter()))
                .cloned()
                .collect(),
        })
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DictionarySchema {
    pub value_type: Box<DbSchema>,
}

impl From<DictionarySchema> for DbSchema {
    fn from(schema: DictionarySchema) -> Self {
        DbSchema::Dictionary(schema)
    }
}

impl DictionarySchema {
    pub fn new<T: Into<DbSchema>>(value_type: T) -> Self {
        Self {
            value_type: Box::new(value_type.into()),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ListSchema {
    pub value_type: Box<DbSchema>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
}

impl From<ListSchema> for DbSchema {
    fn from(schema: ListSchema) -> Self {
        DbSchema::List(schema)
    }
}

impl ListSchema {
    pub fn new<T: Into<DbSchema>>(value_type: T) -> Self {
        Self {
            value_type: Box::new(value_type.into()),
            min_length: None,
            max_length: None,
        }
    }
    pub fn min_length(mut self, min_length: usize) -> Self {
        self.min_length = Some(min_length);
        self
    }
    pub fn max_length(mut self, max_length: usize) -> Self {
        self.max_length = Some(max_length);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SetSchema {
    pub value_type: Box<DbSchema>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
}

impl From<SetSchema> for DbSchema {
    fn from(schema: SetSchema) -> Self {
        DbSchema::Set(schema)
    }
}

impl SetSchema {
    pub fn new<T: Into<DbSchema>>(value_type: T) -> Self {
        Self {
            value_type: Box::new(value_type.into()),
            min_length: None,
            max_length: None,
        }
    }
    pub fn min_length(mut self, min_length: usize) -> Self {
        self.min_length = Some(min_length);
        self
    }
    pub fn max_length(mut self, max_length: usize) -> Self {
        self.max_length = Some(max_length);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct OptionSchema {
    pub value_type: Box<DbSchema>,
    #[serde(default)]
    pub default_set: bool,
}

impl From<OptionSchema> for DbSchema {
    fn from(schema: OptionSchema) -> Self {
        DbSchema::Option(schema)
    }
}

impl OptionSchema {
    pub fn new<T: Into<DbSchema>>(value_type: T) -> Self {
        Self {
            value_type: Box::new(value_type.into()),
            default_set: false,
        }
    }
    pub fn default_set(mut self) -> Self {
        self.default_set = true;
        self
    }
    pub fn set_default(mut self, value: Value) -> Result<Self> {
        match value {
            Value::Null => {
                self.default_set = false;
            }
            _ => {
                self.default_set = true;
                self.value_type = Box::new(self.value_type.set_default(value)?);
            }
        }
        Ok(self)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct EnumSchema {
    pub options: BTreeMap<String, DbSchema>,
    pub default: Option<String>,
    #[serde(default)]
    pub deprecated: BTreeSet<String>,
    #[serde(default)]
    pub format: EnumFormat,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum EnumFormat {
    /// Load and save as { tag: value } (default).
    ExternallyTagged,
    // /// Internally tagged in a field of choice. All values must be
    // /// either units or structs, and common fields must have the
    // /// same type.
    // InternallyTagged(String),
    // /// No tag. The first matching value is selected. Use with caution!
    // Untagged,
    /// Load and save as tag string. By using this format, you
    /// promise the options will *never* include a non-unit value.
    /// In elasticsearch, we set the mapping of enums with format
    /// "as_string" to type "string", meaning they cannot be changed
    /// to another format in a backward-compatible way.
    TagString,
    /// Load options with unit values as string. Non-unit values are
    /// loaded in externally tagged representation ({ tag : string }).
    /// Note that code working with JSON values directly will have to
    /// test for type before testing for non-unit options:
    ///
    ///   ( typeof value === 'object' && tag in value )
    ///
    /// In elasticsearch, all values are saved in externally tagged
    /// representation to ensure backward compatible changes remain
    /// possible.
    UnitAsString,
}

impl Default for EnumFormat {
    fn default() -> Self {
        Self::ExternallyTagged
    }
}

impl From<EnumSchema> for DbSchema {
    fn from(schema: EnumSchema) -> Self {
        DbSchema::Enum(schema)
    }
}

impl EnumSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn option<T: Into<String>, U: Into<DbSchema>>(
        mut self,
        tag: T,
        value_type: U,
    ) -> Self {
        self.options.insert(tag.into(), value_type.into());
        self
    }
    pub fn default<T: Into<String>>(mut self, tag: T) -> Self {
        self.default = Some(tag.into());
        self
    }
    // pub fn internally_tagged<T: Into<String>>(mut self, tag: T) -> Self {
    //     self.format = EnumFormat::InternallyTagged(tag.into());
    //     self
    // }
    pub fn unit_as_string(mut self) -> Self {
        self.format = EnumFormat::UnitAsString;
        self
    }
    pub fn tag_string(mut self) -> Self {
        self.format = EnumFormat::TagString;
        self
    }
    pub fn set_default(mut self, value: Value) -> Result<Self> {
        let (tag, value) = match &self.format {
            EnumFormat::ExternallyTagged => {
                let (tag, val) = value
                    .as_object()
                    .ok_or(Error::InvalidDefaultValue)?
                    .iter()
                    .next()
                    .ok_or(Error::InvalidDefaultValue)?;
                (tag.to_string(), val.clone())
            }
            EnumFormat::UnitAsString => match value.as_str() {
                Some(tag) => (tag.to_string(), json!(())),
                None => {
                    let (tag, val) = value
                        .as_object()
                        .ok_or(Error::InvalidDefaultValue)?
                        .iter()
                        .next()
                        .ok_or(Error::InvalidDefaultValue)?;
                    (tag.to_string(), val.clone())
                }
            },
            EnumFormat::TagString => (
                value
                    .as_str()
                    .ok_or(Error::InvalidDefaultValue)?
                    .to_string(),
                json!(()),
            ),
        };
        let schema = self
            .options
            .remove(&tag)
            .ok_or(Error::InvalidDefaultValue)?;
        self.default = Some(tag.to_string());
        self.options.insert(tag, schema.set_default(value)?);
        Ok(self)
    }

    fn flattened(&self) -> Option<StructSchema> {
        match self.format {
            EnumFormat::ExternallyTagged => Some(StructSchema {
                pk: Vec::new(),
                fields: self
                    .options
                    .iter()
                    .map(|(name, schema)| {
                        let schema = OptionSchema::new(schema.clone()).into();
                        (name.to_string(), schema)
                    })
                    .collect(),
                removed_fields: BTreeSet::new(),
                flatten: BTreeSet::new(),
                elastic_rename: BTreeMap::new(),
            }),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct JsonSchema {
    pub default: Option<Value>,
}

impl From<JsonSchema> for DbSchema {
    fn from(schema: JsonSchema) -> Self {
        DbSchema::Json(schema)
    }
}

impl JsonSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default(mut self, default: Value) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct Ipv4Schema {
    pub default: Option<std::net::Ipv4Addr>,
}

impl From<Ipv4Schema> for DbSchema {
    fn from(schema: Ipv4Schema) -> Self {
        DbSchema::Ipv4(schema)
    }
}

impl Ipv4Schema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default<T: Into<Ipv4Addr>>(mut self, default: T) -> Self {
        self.default = Some(default.into());
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct Ipv6Schema {
    pub default: Option<std::net::Ipv6Addr>,
}

impl From<Ipv6Schema> for DbSchema {
    fn from(schema: Ipv6Schema) -> Self {
        DbSchema::Ipv6(schema)
    }
}

impl Ipv6Schema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default<T: Into<Ipv6Addr>>(mut self, default: T) -> Self {
        self.default = Some(default.into());
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct DateTimeSchema {
    pub default: Option<DateTime<Utc>>,
}

impl From<DateTimeSchema> for DbSchema {
    fn from(schema: DateTimeSchema) -> Self {
        DbSchema::DateTime(schema)
    }
}

impl DateTimeSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default(mut self, default: DateTime<Utc>) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct StringSchema {
    pub default: Option<String>,
}

impl From<StringSchema> for DbSchema {
    fn from(schema: StringSchema) -> Self {
        DbSchema::String(schema)
    }
}

impl StringSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default<T: Into<String>>(mut self, default: T) -> Self {
        self.default = Some(default.into());
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct IntegerSchema {
    pub default: Option<i64>,
}

impl From<IntegerSchema> for DbSchema {
    fn from(schema: IntegerSchema) -> Self {
        DbSchema::Integer(schema)
    }
}

impl IntegerSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default(mut self, default: i64) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct DoubleSchema {
    pub default: Option<f64>,
}

impl From<DoubleSchema> for DbSchema {
    fn from(schema: DoubleSchema) -> Self {
        DbSchema::Double(schema)
    }
}

impl DoubleSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default(mut self, default: f64) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct BoolSchema {
    pub default: Option<bool>,
}

impl From<BoolSchema> for DbSchema {
    fn from(schema: BoolSchema) -> Self {
        DbSchema::Bool(schema)
    }
}

impl BoolSchema {
    pub fn new() -> Self {
        <Self as Default>::default()
    }
    pub fn default(mut self, default: bool) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct UnitSchema {}

impl From<UnitSchema> for DbSchema {
    fn from(schema: UnitSchema) -> Self {
        DbSchema::Unit(schema)
    }
}

impl UnitSchema {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DbSchema {
    pub fn verify(&self) -> Result<()> {
        match self {
            Self::Struct(StructSchema {
                pk,
                flatten,
                fields,
                removed_fields,
                ..
            }) => {
                pk.iter().try_for_each(|key| {
                    match fields.contains_key(key) {
                        true => Ok(()),
                        false => Err(Error::MissingKey(key.to_string())),
                    }
                })?;
                let readded_fields = fields
                    .keys()
                    .filter(|field| removed_fields.contains(*field))
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();

                if !readded_fields.is_empty() {
                    return Err(Error::ReaddedFields(readded_fields));
                }

                fields.iter().try_for_each(|(name, field)| {
                    field
                        .verify()
                        .map_err(|e| e.add_path(format!("field '{name}'")))
                })?;
                flatten
                    .iter()
                    .try_for_each(|field| match fields.get(field) {
                        Some(DbSchema::Struct(_)) => Ok(()),
                        Some(_) => Err(Error::CannotFlatten(field.to_string())),
                        None => {
                            Err(Error::MissingFlattenField(field.to_string()))
                        }
                    })
            }
            DbSchema::Dictionary(DictionarySchema { value_type }) => value_type
                .verify()
                .map_err(|e| e.add_path("value".to_string())),
            DbSchema::List(ListSchema {
                value_type,
                min_length,
                max_length,
            }) => {
                if let (Some(min), Some(max)) = (min_length, max_length) {
                    if max < min {
                        return Err(Error::MaxSmallerThanMinLength);
                    }
                }
                value_type
                    .verify()
                    .map_err(|e| e.add_path("element".to_string()))
            }
            DbSchema::Set(SetSchema {
                value_type,
                min_length,
                max_length,
            }) => {
                if let (Some(min), Some(max)) = (min_length, max_length) {
                    if max < min {
                        return Err(Error::MaxSmallerThanMinLength);
                    }
                }
                value_type
                    .verify()
                    .map_err(|e| e.add_path("element".to_string()))?;
                match value_type.is_hashable() {
                    true => Ok(()),
                    false => Err(Error::UnhashableValue),
                }
            }
            DbSchema::Option(OptionSchema { value_type, .. }) => value_type
                .verify()
                .map_err(|e| e.add_path("option value".to_string())),
            DbSchema::Enum(EnumSchema {
                options,
                default,
                format,
                deprecated,
            }) => {
                if let Some(option) = default {
                    if !options.contains_key(option) {
                        return Err(Error::MissingDefaultOption(
                            option.to_string(),
                        ));
                    }
                    if deprecated.contains(option) {
                        return Err(Error::DeprecatedOption(
                            option.to_string(),
                        ));
                    }
                }

                deprecated.iter().try_for_each(|option| {
                    match options.contains_key(option) {
                        true => Ok(()),
                        false => Err(Error::MissingDeprecatedOption(
                            option.to_string(),
                        )),
                    }
                })?;

                match format {
                    EnumFormat::ExternallyTagged | EnumFormat::UnitAsString => {
                        /* no extra checks */
                    }
                    /* EnumFormat::InternallyTagged => { /* verify option compatibility */ } */
                    EnumFormat::TagString => {
                        options.iter().try_for_each(|(name, option)| {
                            match option {
                                DbSchema::Unit(_) => Ok(()),
                                _ => Err(Error::NonUnitOptionInTagString(
                                    name.to_string(),
                                )),
                            }
                        })?
                    }
                }
                options.iter().try_for_each(|(name, option)| {
                    option
                        .verify()
                        .map_err(|e| e.add_path(format!("option '{name}'")))
                })
            }
            DbSchema::Json(JsonSchema { default: _ }) => {
                /* Default Value is valid JSON by definition. */
                Ok(())
            }
            DbSchema::Ipv4(Ipv4Schema { default: _ }) => Ok(()),
            DbSchema::Ipv6(Ipv6Schema { default: _ }) => Ok(()),
            DbSchema::DateTime(DateTimeSchema { default: _ }) => Ok(()),
            DbSchema::String(StringSchema { default: _ }) => Ok(()),
            DbSchema::Integer(IntegerSchema { default: _ }) => Ok(()),
            DbSchema::Double(DoubleSchema { default: _ }) => Ok(()),
            DbSchema::Bool(BoolSchema { default: _ }) => Ok(()),
            DbSchema::Unit(UnitSchema {}) => Ok(()),
        }
    }

    pub fn verify_value(&self, value: &Value) -> Result<()> {
        match self {
            DbSchema::Struct(StructSchema { fields, .. }) => {
                let map = value.as_object_or_err()?;
                map.iter().try_for_each(|(name, _)| {
                    match fields.contains_key(name) {
                        true => Ok(()),
                        false => Err(Error::UnknownField(name.to_string())),
                    }
                })?;
                fields.iter().try_for_each(|(name, field)| {
                    let null = Value::Null;
                    let value = match map.get(name) {
                        Some(v) => v,
                        None => match field {
                            DbSchema::Option(_) => &null,
                            _ => {
                                return Err(Error::MissingField(
                                    name.to_string(),
                                ))
                            }
                        },
                    };
                    field
                        .verify_value(value)
                        .map_err(|e| e.add_path(format!("field '{name}'")))
                })
            }
            DbSchema::Dictionary(DictionarySchema { value_type }) => {
                let map = value.as_object_or_err()?;
                map.iter().try_for_each(|(key, value)| {
                    value_type
                        .verify_value(value)
                        .map_err(|e| e.add_path(format!("key '{key}'")))
                })
            }
            DbSchema::Enum(EnumSchema {
                options,
                format,
                deprecated,
                ..
            }) => {
                let (tag, value) = value.as_enum_or_err(format)?;
                if deprecated.contains(tag) {
                    return Err(Error::DeprecatedOption(tag.to_string()));
                }
                options
                    .get(tag)
                    .ok_or_else(|| Error::MissingOption(tag.to_string()))?
                    .verify_value(value)
                    .map_err(|e| e.add_path(format!("option '{tag}'")))
            }
            DbSchema::List(ListSchema {
                value_type,
                min_length,
                max_length,
            }) => {
                let list = value.as_array_or_err()?;
                if let Some(min) = min_length {
                    if list.len() < *min {
                        return Err(Error::ListTooShort(*min, list.len()));
                    }
                }
                if let Some(max) = max_length {
                    if list.len() > *max {
                        return Err(Error::ListTooLong(*max, list.len()));
                    }
                }
                list.iter().enumerate().try_for_each(|(i, value)| {
                    value_type
                        .verify_value(value)
                        .map_err(|e| e.add_path(format!("element #{i}")))
                })
            }
            DbSchema::Set(SetSchema {
                value_type,
                min_length,
                max_length,
            }) => {
                let list = value.as_array_or_err()?;
                if let Some(min) = min_length {
                    if list.len() < *min {
                        return Err(Error::ListTooShort(*min, list.len()));
                    }
                }
                if let Some(max) = max_length {
                    if list.len() > *max {
                        return Err(Error::ListTooLong(*max, list.len()));
                    }
                }
                /* todo: verify uniqueness */
                list.iter().enumerate().try_for_each(|(i, value)| {
                    value_type
                        .verify_value(value)
                        .map_err(|e| e.add_path(format!("element #{i}")))
                })
            }
            DbSchema::Option(OptionSchema { value_type, .. }) => match value {
                Value::Null => Ok(()),
                _ => value_type
                    .verify_value(value)
                    .map_err(|e| e.add_path("option value".to_string())),
            },
            DbSchema::Json(JsonSchema { .. }) => Ok(()),
            DbSchema::Ipv4(Ipv4Schema { .. }) => {
                let value = value.as_str_or_err()?;
                match Ipv4Addr::from_str(value) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Error::InvalidIpv4Addr(value.to_string(), e)),
                }
            }
            DbSchema::Ipv6(Ipv6Schema { .. }) => {
                let value = value.as_str_or_err()?;
                match Ipv6Addr::from_str(value) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Error::InvalidIpv6Addr(value.to_string(), e)),
                }
            }
            DbSchema::DateTime(DateTimeSchema { .. }) => {
                let value = value.as_str_or_err()?;
                match DateTime::parse_from_rfc3339(value) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Error::InvalidDateTime(value.to_string(), e)),
                }
            }
            DbSchema::String(StringSchema { .. }) => {
                value.as_str_or_err().map(|_| ())
            }
            DbSchema::Integer(IntegerSchema { .. }) => {
                value.as_i64_or_err().map(|_| ())
            }
            DbSchema::Double(DoubleSchema { .. }) => {
                value.as_f64_or_err().map(|_| ())
            }
            DbSchema::Bool(BoolSchema { .. }) => {
                value.as_bool_or_err().map(|_| ())
            }
            DbSchema::Unit(UnitSchema { .. }) => {
                value.as_null_or_err().map(|_| ())
            }
        }
    }

    /// Deprecated: use verify_compatibility, which allows for reindexes.
    /// Check if the schema is backward-compatible with a previous schema with
    /// regard to data format. If this functions returns Ok(()), any data saved
    /// under the previous schema should still be valid under the current schema.
    pub fn verify_backward_compatibility(&self, previous: &Self) -> Result<()> {
        match (self, previous) {
            (DbSchema::Struct(to),
			 DbSchema::Struct(from)) => {

				let to = to.flattened()?;
				let from = from.flattened()?;

				let unremoved_fields = (&from.removed_fields - &to.removed_fields)
					.into_iter()
					.collect::<Vec<_>>();

				if !unremoved_fields.is_empty() {
					return Err(Error::UnremovedFields(unremoved_fields));
				}

        		let to_fields : BTreeSet<&str> = to.fields.keys().map(String::as_ref).collect();
				let from_fields : BTreeSet<&str> = from.fields.keys().map(String::as_ref).collect();

				let added_fields_without_default = (&to_fields - &from_fields)
					.into_iter()
					.filter(|field| {
						let schema = to.fields.get(*field).unwrap();
						schema.default().is_none()
					})
					.map(str::to_string)
					.collect::<Vec<_>>();

				if !added_fields_without_default.is_empty() {
					return Err(Error::AddedFieldsWithoutDefault(added_fields_without_default));
				}

				let unlisted_removed_fields = (&from_fields - &to_fields)
					.into_iter()
					.filter(|field| !to.removed_fields.contains(*field))
					.map(str::to_string)
					.collect::<Vec<_>>();

				if !unlisted_removed_fields.is_empty() {
					return Err(Error::UnlistedRemovedFields(unlisted_removed_fields));
				}

				(&from_fields & &to_fields).into_iter().try_for_each(|field| {
					let from_schema = from.fields.get(field).unwrap();
					let to_schema = to.fields.get(field).unwrap();
					to_schema
						.verify_backward_compatibility(from_schema)
						.map_err(|e| e.add_path(field))
				})?;
				Ok(())

			},
        	(DbSchema::Enum(to),
        	 DbSchema::Enum(from)) => {

				match (&to.format, &from.format) {
					(EnumFormat::ExternallyTagged | EnumFormat::UnitAsString,
					 EnumFormat::ExternallyTagged | EnumFormat::UnitAsString)
						| (EnumFormat::TagString, EnumFormat::TagString) => Ok(()),
					_ => Err(Error::InvalidEnumFormatTransition)
				}?;

        		let to_options : BTreeSet<&str> = to.options.keys().map(String::as_ref).collect();
				let from_options : BTreeSet<&str> = from.options.keys().map(String::as_ref).collect();

				let removed_options = (&from_options - &to_options)
					.into_iter()
					.map(str::to_string)
					.collect::<Vec<_>>();

				if !removed_options.is_empty() {
					return Err(Error::RemovedOptions(removed_options));
				}

				(&from_options & &to_options)
					.into_iter()
					.try_for_each(|option| {
						let from_schema = from.options.get(option).unwrap();
						let to_schema = to.options.get(option).unwrap();
						to_schema
							.verify_backward_compatibility(from_schema)
							.map_err(|e| e.add_path(option))
					})

			},
            (DbSchema::Dictionary(DictionarySchema {value_type: to, ..}),
        	 DbSchema::Dictionary(DictionarySchema{value_type: from, ..})) => {
        		to.verify_backward_compatibility(from)
        			.map_err(|e| e.add_path("value".to_string()))
        	},
        	(DbSchema::Set(SetSchema {value_type: to,
        							  min_length: to_min_length,
        							  max_length: to_max_length,
        							  ..}),
        	 DbSchema::Set(SetSchema {value_type: from,
        							  min_length: from_min_length,
        							  max_length: from_max_length,
        							  ..}))
        		| (DbSchema::List(ListSchema {value_type: to,
        									  min_length: to_min_length,
        									  max_length: to_max_length,
        									  ..}),
        		   DbSchema::List(ListSchema {value_type: from,
        									  min_length: from_min_length,
        									  max_length: from_max_length,
        									  ..})
        		   | DbSchema::Set(SetSchema {value_type: from,
        									  min_length: from_min_length,
        									  max_length: from_max_length,
        									  ..})) => {
        			match (to_min_length, from_min_length) {
        				(Some(_), None) => Err(Error::AddedMinLength),
        				(Some(to), Some(from)) if to > from
        					=> Err(Error::IncreasedMinLength(*to, *from)),
        				_ => Ok(()),
        			}?;
        			match (to_max_length, from_max_length) {
        				(Some(_), None) => Err(Error::AddedMaxLength),
        				(Some(to), Some(from)) if to < from
        					=> Err(Error::DecreasedMaxLength(*to, *from)),
        				_ => Ok(()),
        			}?;
        			to.verify_backward_compatibility(from)
        				.map_err(|e| e.add_path("elem".to_string()))
        		},
			(DbSchema::Option(OptionSchema { value_type: to, ..}),
        	 DbSchema::Option(OptionSchema { value_type: from, ..})) => {
        		to.verify_backward_compatibility(from)
        			.map_err(|e| e.add_path("some".to_string()))
        	},
            (DbSchema::Option(OptionSchema { value_type: to, ..}),
        	 from) => {
        		to.verify_backward_compatibility(from)
        			.map_err(|e| e.add_path("some".to_string()))
        	},
        	/* For Json values, backward compatibility cannot be checked.
        	 * In this case, it is the user's responsibility. */
            (DbSchema::Json(_), DbSchema::Json(_))
        		| (DbSchema::Ipv4(_), DbSchema::Ipv4(_))
        		| (DbSchema::Ipv6(_), DbSchema::Ipv6(_))
        		| (DbSchema::DateTime(_), DbSchema::DateTime(_))
        	/* If the user puts machine-interpretable info in String,
        	 * it is up to the user to guarantee compatibility. */
        		| (DbSchema::String(_), DbSchema::String(_))
        		| (DbSchema::Integer(_), DbSchema::Integer(_))
        		| (DbSchema::Double(_), DbSchema::Double(_))
        		| (DbSchema::Bool(_), DbSchema::Bool(_))
        		| (DbSchema::Unit(_), DbSchema::Unit(_)) => Ok(()),
            (to, from) => Err(Error::IncompatibleKinds(to.kind(), from.kind())),
        }
    }

    /// Verify compatibility of this schema with a previous schema.
    pub fn verify_compatibility(
        &self,
        previous: &Self,
    ) -> Result<Compatibility> {
        match (self, previous) {
            (DbSchema::Struct(to),
			 DbSchema::Struct(from)) => {

				let to = to.flattened()?;
				let from = from.flattened()?;

				let unremoved_fields = (&from.removed_fields - &to.removed_fields)
					.into_iter()
					.collect::<Vec<_>>();

				if !unremoved_fields.is_empty() {
					return Err(Error::UnremovedFields(unremoved_fields));
				}

        		let to_fields = to.fields.keys().cloned().collect::<BTreeSet<_>>();
				let from_fields = from.fields.keys().cloned().collect::<BTreeSet<_>>();

				let added_fields_without_default = (&to_fields - &from_fields)
					.into_iter()
					.filter(|field| {
						let schema = to.fields.get(field).unwrap();
						schema.default().is_none()
					})
					.collect::<Vec<_>>();

				if !added_fields_without_default.is_empty() {
					return Err(Error::AddedFieldsWithoutDefault(added_fields_without_default));
				}

				let unlisted_removed_fields = (&from_fields - &to_fields)
					.into_iter()
					.filter(|field| !to.removed_fields.contains(field))
					.collect::<Vec<_>>();

				if !unlisted_removed_fields.is_empty() {
					return Err(Error::UnlistedRemovedFields(unlisted_removed_fields));
				}

				(&from_fields & &to_fields).into_iter().try_fold(Compatibility::Compatible, |res,field| {
					let from_schema = from.fields.get(&field).unwrap();
					let to_schema = to.fields.get(&field).unwrap();
					Ok(res.max(to_schema
						.verify_compatibility(from_schema)
						.map_err(|e| e.add_path(field))?))
				})
			},
        	(DbSchema::Enum(to),
        	 DbSchema::Enum(from)) => {

				let compat = match (&to.format, &from.format) {
					(EnumFormat::ExternallyTagged | EnumFormat::UnitAsString,
					 EnumFormat::ExternallyTagged | EnumFormat::UnitAsString)
						| (EnumFormat::TagString, EnumFormat::TagString) => Compatibility::Compatible,
					_ => Compatibility::NeedsReindex
				};

        		let to_options : BTreeSet<&str> = to.options.keys().map(String::as_ref).collect();
				let from_options : BTreeSet<&str> = from.options.keys().map(String::as_ref).collect();

				let removed_options = (&from_options - &to_options)
					.into_iter()
					.map(str::to_string)
					.collect::<Vec<_>>();

				if !removed_options.is_empty() {
					return Err(Error::RemovedOptions(removed_options));
				}

				(&from_options & &to_options)
					.into_iter()
					.map(|option| {
						let from_schema = from.options.get(option).unwrap();
						let to_schema = to.options.get(option).unwrap();
						to_schema
							.verify_compatibility(from_schema)
							.map_err(|e| e.add_path(option))
					}).try_fold(compat, |a,b| Ok(a.max(b?)))

			},
            (DbSchema::Dictionary(DictionarySchema {value_type: to, ..}),
        	 DbSchema::Dictionary(DictionarySchema{value_type: from, ..})) => {
        		to.verify_compatibility(from)
        			.map_err(|e| e.add_path("value".to_string()))
        	},
        	(DbSchema::Set(SetSchema {value_type: to,
        							  min_length: to_min_length,
        							  max_length: to_max_length,
        							  ..}),
        	 DbSchema::Set(SetSchema {value_type: from,
        							  min_length: from_min_length,
        							  max_length: from_max_length,
        							  ..}))
        		| (DbSchema::List(ListSchema {value_type: to,
        									  min_length: to_min_length,
        									  max_length: to_max_length,
        									  ..}),
        		   DbSchema::List(ListSchema {value_type: from,
        									  min_length: from_min_length,
        									  max_length: from_max_length,
        									  ..})
        		   | DbSchema::Set(SetSchema {value_type: from,
        									  min_length: from_min_length,
        									  max_length: from_max_length,
        									  ..})) => {
        			match (to_min_length, from_min_length) {
        				(Some(_), None) => Err(Error::AddedMinLength),
        				(Some(to), Some(from)) if to > from
        					=> Err(Error::IncreasedMinLength(*to, *from)),
        				_ => Ok(()),
        			}?;
        			match (to_max_length, from_max_length) {
        				(Some(_), None) => Err(Error::AddedMaxLength),
        				(Some(to), Some(from)) if to < from
        					=> Err(Error::DecreasedMaxLength(*to, *from)),
        				_ => Ok(()),
        			}?;
        			to.verify_compatibility(from)
        				.map_err(|e| e.add_path("elem".to_string()))
        		},
			(DbSchema::Option(OptionSchema { value_type: to, ..}),
        	 DbSchema::Option(OptionSchema { value_type: from, ..})) => {
        		to.verify_compatibility(from)
        			.map_err(|e| e.add_path("some".to_string()))
        	},
            (DbSchema::Option(OptionSchema { value_type: to, ..}),
        	 from) => {
        		to.verify_compatibility(from)
        			.map_err(|e| e.add_path("some".to_string()))
        	},
        	/* For Json values, backward compatibility cannot be checked.
        	 * In this case, it is the user's responsibility. */
            (DbSchema::Json(_), DbSchema::Json(_))
        		| (DbSchema::Ipv4(_), DbSchema::Ipv4(_))
        		| (DbSchema::Ipv6(_), DbSchema::Ipv6(_))
        		| (DbSchema::DateTime(_), DbSchema::DateTime(_))
        	/* If the user puts machine-interpretable info in String,
        	 * it is up to the user to guarantee compatibility. */
        		| (DbSchema::String(_), DbSchema::String(_))
        		| (DbSchema::Integer(_), DbSchema::Integer(_))
        		| (DbSchema::Double(_), DbSchema::Double(_))
        		| (DbSchema::Bool(_), DbSchema::Bool(_))
        		| (DbSchema::Unit(_), DbSchema::Unit(_)) => Ok(Compatibility::Compatible),
            (to, from) => Err(Error::IncompatibleKinds(to.kind(), from.kind())),
        }
    }

    pub fn is_hashable(&self) -> bool {
        true /* todo */
    }

    pub fn default(&self) -> Option<Value> {
        match self {
            Self::Struct(StructSchema { fields, .. }) => {
                let mut map: Map<String, Value> =
                    Map::with_capacity(fields.len());
                for (k, v) in fields {
                    map.insert(k.clone(), v.default()?);
                }
                Some(Value::Object(map))
            }
            Self::Dictionary(_) => Some(json!({})),
            Self::List(_) => Some(json!([])),
            Self::Set(_) => Some(json!([])),
            Self::Option(OptionSchema {
                value_type,
                default_set,
                ..
            }) => match default_set {
                true => value_type.default(),
                false => Some(Value::Null),
            },
            Self::Enum(EnumSchema {
                options,
                default,
                format,
                ..
            }) => default.as_ref().and_then(|option| {
                options.get(option).and_then(|option_schema| {
                    option_schema.default().map(|value| match format {
                        EnumFormat::ExternallyTagged => {
                            json!({ option: value })
                        }
                        EnumFormat::UnitAsString => match option_schema {
                            DbSchema::Unit(_) => json!(option),
                            _ => json!({ option: value }),
                        },
                        EnumFormat::TagString => json!(option),
                    })
                })
            }),
            Self::Json(JsonSchema { default }) => default.clone(),
            Self::Ipv4(Ipv4Schema { default }) => default.map(|v| json!(v)),
            Self::Ipv6(Ipv6Schema { default }) => default.map(|v| json!(v)),
            Self::DateTime(DateTimeSchema { default }) => default
                .map(|v| json!(v.to_rfc3339_opts(SecondsFormat::Secs, true))),
            Self::String(StringSchema { default }) => {
                default.as_ref().map(|v| json!(v))
            }
            Self::Integer(IntegerSchema { default }) => {
                default.map(|v| json!(v))
            }
            Self::Double(DoubleSchema { default }) => default.map(|v| json!(v)),
            Self::Bool(BoolSchema { default }) => default.map(|v| json!(v)),
            Self::Unit(_) => Some(Value::Null),
        }
    }

    /// Set the default for the schema. Used during schema auto-derivation.
    /// Panics if the value is not correct or the schema is not yet supported!
    pub fn set_default<T: Serialize>(self, value: T) -> Result<Self> {
        let value = serde_json::to_value(value)
            .expect("failed to serialize default value");
        match self {
            DbSchema::Struct(s) => Ok(DbSchema::Struct(s.set_default(value)?)),
            DbSchema::Enum(s) => Ok(DbSchema::Enum(s.set_default(value)?)),
            DbSchema::Dictionary(_) => todo!(),
            DbSchema::List(_) => todo!(),
            DbSchema::Set(_) => todo!(),
            DbSchema::Option(s) => Ok(DbSchema::Option(s.set_default(value)?)),
            DbSchema::Json(_) => todo!(),
            DbSchema::Ipv4(_) => todo!(),
            DbSchema::Ipv6(_) => todo!(),
            DbSchema::DateTime(_) => todo!(),
            DbSchema::String(s) => Ok(DbSchema::String(
                s.default(value.as_str().ok_or(Error::InvalidDefaultValue)?),
            )),
            DbSchema::Integer(s) => Ok(DbSchema::Integer(
                s.default(value.as_i64().ok_or(Error::InvalidDefaultValue)?),
            )),
            DbSchema::Double(s) => Ok(DbSchema::Double(
                s.default(value.as_f64().ok_or(Error::InvalidDefaultValue)?),
            )),
            DbSchema::Bool(s) => Ok(DbSchema::Bool(
                s.default(value.as_bool().ok_or(Error::InvalidDefaultValue)?),
            )),
            DbSchema::Unit(s) => {
                value.as_null().ok_or(Error::InvalidDefaultValue)?;
                Ok(DbSchema::Unit(s))
            }
        }
    }

    // pub fn hashable(&self, value: &Value) -> Option<HashValue> {

    // }
}

/* Standard HasSchema impls. */

impl HasSchema for String {
    fn schema() -> DbSchema {
        StringSchema::new().into()
    }
}

impl HasSchema for &str {
    fn schema() -> DbSchema {
        StringSchema::new().into()
    }
}

impl HasSchema for Cow<'_, str> {
    fn schema() -> DbSchema {
        StringSchema::new().into()
    }
}

impl HasSchema for i64 {
    fn schema() -> DbSchema {
        IntegerSchema::new().into()
    }
}

impl HasSchema for f64 {
    fn schema() -> DbSchema {
        DoubleSchema::new().into()
    }
}

impl HasSchema for bool {
    fn schema() -> DbSchema {
        BoolSchema::new().into()
    }
}

impl HasSchema for () {
    fn schema() -> DbSchema {
        UnitSchema::new().into()
    }
}

impl HasSchema1 for Value {
    fn schema1(value: DbSchema) -> DbSchema {
        value
    }
}

impl<T: HasSchema> HasSchema for Option<T> {
    fn schema() -> DbSchema {
        OptionSchema::new(T::schema()).into()
    }
}

impl<T: HasSchema, E: HasSchema> HasSchema for std::result::Result<T, E> {
    fn schema() -> DbSchema {
        EnumSchema::new()
            .option("Ok", T::schema())
            .option("Err", E::schema())
            .into()
    }
}

impl<T: HasSchema> HasSchema for Vec<T> {
    fn schema() -> DbSchema {
        ListSchema::new(T::schema()).into()
    }
}

impl<T: HasSchema> HasSchema for HashSet<T> {
    fn schema() -> DbSchema {
        SetSchema::new(T::schema()).into()
    }
}

impl<T: HasSchema> HasSchema for BTreeSet<T> {
    fn schema() -> DbSchema {
        SetSchema::new(T::schema()).into()
    }
}

impl<K: HasSchema, V: HasSchema> HasSchema for HashMap<K, V> {
    fn schema() -> DbSchema {
        assert!(matches!(K::schema(), DbSchema::String(_)));
        DictionarySchema::new(V::schema()).into()
    }
}

impl<K: HasSchema, V: HasSchema> HasSchema for BTreeMap<K, V> {
    fn schema() -> DbSchema {
        assert!(matches!(K::schema(), DbSchema::String(_)));
        DictionarySchema::new(V::schema()).into()
    }
}
