/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::borrow::Cow;
use std::marker::PhantomData;

use serde::de::{MapAccess, Visitor};
use serde::{forward_to_deserialize_any, Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{Compatibility, EnumSchema};

use super::error::Result;
use super::schema::{DbSchema, HasSchema1, StructSchema};
use super::versioning::{
    DualVersionedValue, Identified, SingleVersionedValue, Timestamped,
    VersioningType,
};

pub trait HasTableDef {
    fn table_def() -> DbTable;
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
#[serde(transparent)]
pub struct DbTableId(Cow<'static, str>);

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct DbTable {
    pub versioning: VersioningType,
    pub schema: NamedFieldSchema,
    #[serde(default)]
    pub force_update: bool,
}

#[derive(Serialize, PartialEq, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NamedFieldSchema {
    Struct(StructSchema),
    Enum(EnumSchema),
}

impl From<NamedFieldSchema> for DbSchema {
    fn from(value: NamedFieldSchema) -> Self {
        match value {
            NamedFieldSchema::Struct(v) => v.into(),
            NamedFieldSchema::Enum(v) => v.into(),
        }
    }
}

/// Deserialize with default tag.
impl<'de> Deserialize<'de> for NamedFieldSchema {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NamedFieldSchemaVisitor;

        impl<'de> Visitor<'de> for NamedFieldSchemaVisitor {
            type Value = NamedFieldSchema;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(formatter, "NamedFieldSchema enum")
            }

            fn visit_map<A>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut fields = Vec::new();

                while let Some(field) = map.next_key::<Key>()? {
                    match field.0.as_ref() {
                        "type" => {
                            return match map.next_value::<Key>()?.0.as_ref() {
                                "struct" => Ok(NamedFieldSchema::Struct(
                                    StructSchema::deserialize(
                                        FieldsDeserializer::new(fields, map),
                                    )?,
                                )),
                                "enum" => Ok(NamedFieldSchema::Enum(
                                    EnumSchema::deserialize(
                                        FieldsDeserializer::new(fields, map),
                                    )?,
                                )),
                                _ => {
                                    Err(<A::Error as serde::de::Error>::custom(
                                        "invalid type",
                                    ))
                                }
                            }
                        }
                        _ => {
                            fields.push((
                                field.0,
                                map.next_value::<serde_value::Value>()?,
                            ));
                        }
                    }
                }

                Ok(NamedFieldSchema::Struct(StructSchema::deserialize(
                    FieldsDeserializer::new(fields, map),
                )?))
            }
        }

        deserializer.deserialize_map(NamedFieldSchemaVisitor)
    }
}

struct Key<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct KeyVisitor<'de>(PhantomData<&'de ()>);

        impl<'de> Visitor<'de> for KeyVisitor<'de> {
            type Value = Key<'de>;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(formatter, "a Key")
            }

            fn visit_borrowed_str<E>(
                self,
                v: &'de str,
            ) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Key(Cow::Borrowed(v)))
            }

            fn visit_string<E>(
                self,
                v: String,
            ) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Key(Cow::Owned(v)))
            }

            fn visit_str<E>(
                self,
                v: &str,
            ) -> std::prelude::v1::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(v.to_string())
            }
        }

        deserializer.deserialize_str(KeyVisitor(PhantomData))
    }
}

struct FieldsDeserializer<'de, A>(
    std::vec::IntoIter<(Cow<'de, str>, serde_value::Value)>,
    Option<serde_value::Value>,
    A,
);

impl<'de, A> FieldsDeserializer<'de, A> {
    fn new(fields: Vec<(Cow<'de, str>, serde_value::Value)>, map: A) -> Self {
        Self(fields.into_iter(), None, map)
    }
}

impl<'de, A: MapAccess<'de>> Deserializer<'de> for FieldsDeserializer<'de, A> {
    type Error = A::Error;

    fn deserialize_any<V>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for FieldsDeserializer<'de, A> {
    type Error = A::Error;

    fn next_key_seed<K>(
        &mut self,
        seed: K,
    ) -> std::result::Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if let Some((key, value)) = self.0.next() {
            self.1 = Some(value);
            match key {
                Cow::Owned(s) => Ok(Some(seed.deserialize(
                    serde::de::value::StringDeserializer::new(s),
                )?)),
                Cow::Borrowed(s) => {
                    Ok(Some(seed.deserialize(
                        serde::de::value::StrDeserializer::new(s),
                    )?))
                }
            }
        } else {
            self.2.next_key_seed(seed)
        }
    }

    fn next_value_seed<V>(
        &mut self,
        seed: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        if let Some(value) = self.1.take() {
            seed.deserialize(serde_value::ValueDeserializer::new(value))
        } else {
            self.2.next_value_seed(seed)
        }
    }
}

impl DbTableId {
    pub fn new(name: &str) -> Self {
        Self(Cow::Owned(name.to_string()))
    }
    pub fn to_str(&self) -> &str {
        &self.0
    }
    pub fn from_string(id: String) -> Self {
        Self(Cow::Owned(id))
    }
    pub const fn from_static(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }
}

impl std::fmt::Display for DbTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl DbTable {
    /// Deprecated; use verify_compatibility instead.
    pub fn verify_backward_compatibility(&self, previous: &Self) -> Result<()> {
        self.schema()
            .verify_backward_compatibility(&previous.schema())
    }
    pub fn verify_compatibility(
        &self,
        previous: &Self,
    ) -> Result<Compatibility> {
        self.schema().verify_compatibility(&previous.schema())
    }

    pub fn sort_fields(&self) -> Value {
        self.versioning.sort_fields()
    }

    pub fn schema(&self) -> DbSchema {
        match &self.versioning {
            VersioningType::Timestamped => {
                Timestamped::schema1(self.value_schema())
            }
            VersioningType::SingleTimeline => {
                Identified::<SingleVersionedValue>::schema1(self.value_schema())
            }
            VersioningType::DualTimeline => {
                Identified::<DualVersionedValue>::schema1(self.value_schema())
            }
        }
    }
    pub fn value_schema(&self) -> DbSchema {
        self.schema.clone().into()
    }
}
