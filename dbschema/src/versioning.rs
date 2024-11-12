/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    schema::HasSchema1, table::HasTableDef, DbTable, HasSchema, StringSchema,
};

use super::schema::{DateTimeSchema, DbSchema, OptionSchema, StructSchema};

#[derive(
    Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug,
)]
#[serde(transparent)]
pub struct ObjectId(Cow<'static, str>);

#[derive(Serialize, Deserialize, Clone, Debug, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersioningType {
    /// Every row gets a timestamp.
    Timestamped,
    /// Every row gets an object_id and a timeline anchor
    /// (@active).
    SingleTimeline,
    /// Every row gets an object_id and two timeline anchors
    /// (@current and @active).
    DualTimeline,
}

#[derive(Serialize, Deserialize, Clone, Debug, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Timeline {
    Current,
    Active,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Anchor {
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
    pub created: DateTime<Utc>,
    //pub modified: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct TimeRange {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}
pub type TimestampedValue = Timestamped<Value>;
pub type SingleVersionedValue = SingleVersioned<Value>;
pub type DualVersionedValue = DualVersioned<Value>;
pub type SingleVersioned<T> = Versioned<SingleVersionInfo, T>;
pub type DualVersioned<T> = Versioned<DualVersionInfo, T>;

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Timestamped<T> {
    pub timestamp: DateTime<Utc>,
    pub value: T,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Identified<T> {
    pub object_id: ObjectId,
    pub value: T,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Versioned<V, T> {
    pub version: V,
    pub value: T,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SingleVersionInfo {
    pub active: Anchor,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DualVersionInfo {
    pub current: Anchor,
    pub active: Option<Anchor>,
    pub committed: Option<DateTime<Utc>>,
}

impl VersioningType {
    pub fn sort_fields(&self) -> Value {
        match self {
            Self::Timestamped => {
                json!(["@timestamp", "object_id.keyword"])
            }
            Self::SingleTimeline => {
                json!(["@active.from", "object_id.keyword"])
            }
            Self::DualTimeline => {
                json!(["@current.from", "object_id.keyword"])
            }
        }
    }
}

impl<T> SingleVersioned<T> {
    /// Create a new object valid from `now`.
    pub fn new(now: DateTime<Utc>, value: T) -> Self {
        let version = SingleVersionInfo::new(now);
        Self { version, value }
    }
    /// Update the object `now`, returning the previous (closed)
    /// object.
    pub fn update(self, now: DateTime<Utc>, value: T) -> Self {
        Self {
            version: self.version.update(now),
            value,
        }
    }
    /// Remove (close) the object.
    pub fn remove(self, now: DateTime<Utc>) -> Self {
        Self {
            version: self.version.remove(now),
            value: self.value,
        }
    }
}

impl<T> DualVersioned<T> {
    /// Create a new object "current" from `now`.
    pub fn new(now: DateTime<Utc>, value: T, commit: bool) -> Self {
        Self {
            version: DualVersionInfo::new(now, commit),
            value,
        }
    }

    /// Update the object "current" from `now`, removing any  "active" anchor.
    pub fn update(self, now: DateTime<Utc>, value: T, commit: bool) -> Self {
        Self {
            version: self.version.update(now, commit),
            value,
        }
    }

    /// Update the object "current" from `now`, removing any  "active" anchor.
    pub fn update_uncommitted(
        self,
        now: DateTime<Utc>,
        value: T,
        commit: bool,
    ) -> Self {
        Self {
            version: self.version.update_uncommitted(now, commit),
            value,
        }
    }

    pub fn remove(self, now: DateTime<Utc>) -> Self {
        Self {
            version: self.version.remove(now),
            value: self.value,
        }
    }

    pub fn activate(self, now: DateTime<Utc>, prev: Option<&Anchor>) -> Self {
        Self {
            version: self.version.activate(now, prev),
            value: self.value,
        }
    }

    pub fn activate_remove(self, now: DateTime<Utc>) -> Self {
        Self {
            version: self.version.activate_remove(now),
            value: self.value,
        }
    }
}

impl SingleVersionInfo {
    /// Create a new version, valid from `now`.
    pub fn new(now: DateTime<Utc>) -> Self {
        let active = Anchor::new(now);
        Self { active }
    }
    /// Update the version `now`.
    pub fn update(self, now: DateTime<Utc>) -> Self {
        Self {
            active: self.active.update(now),
        }
    }
    // /// Update the version `now`, returning the previous (closed)
    // /// version.
    // pub fn updated(&mut self, now: DateTime<Utc>) -> Self {
    //     let active = self.active.updated(now);
    //     Self { active }
    // }
    /// Close the version.
    pub fn remove(self, now: DateTime<Utc>) -> Self {
        let active = self.active.remove(now);
        Self { active }
    }
}

impl DualVersionInfo {
    /// Create a new version, "current" from `now`.
    pub fn new(now: DateTime<Utc>, commit: bool) -> Self {
        let current = Anchor::new(now);
        Self {
            current,
            active: None,
            committed: commit.then_some(now),
        }
    }
    pub fn update(self, now: DateTime<Utc>, commit: bool) -> Self {
        Self {
            current: self.current.update(now),
            active: None,
            committed: commit.then_some(now),
        }
    }
    pub fn update_uncommitted(self, now: DateTime<Utc>, commit: bool) -> Self {
        debug_assert!(self.active.is_none());
        Self {
            current: self.current,
            active: None,
            committed: commit.then_some(now),
        }
    }
    pub fn remove(self, now: DateTime<Utc>) -> Self {
        debug_assert!(self
            .active
            .as_ref()
            .map_or(true, |active| active.to.is_none()));
        Self {
            current: self.current.remove(now),
            active: self.active,
            committed: self.committed.or(Some(now)),
        }
    }
    pub fn activate(self, now: DateTime<Utc>, active: Option<&Anchor>) -> Self {
        debug_assert!(self.active.is_none());
        Self {
            current: self.current,
            active: Some(
                active
                    .map_or_else(|| Anchor::new(now), |active| active.clone())
                    .update(now),
            ),
            committed: self.committed.or(Some(now)),
        }
    }
    pub fn activate_remove(self, now: DateTime<Utc>) -> Self {
        debug_assert!(self
            .active
            .as_ref()
            .map_or(false, |active| active.to.is_none()));
        Self {
            current: self.current,
            active: Some(
                self.active.unwrap_or_else(|| Anchor::new(now)).remove(now),
            ),
            committed: self.committed.or(Some(now)),
        }
    }
}

impl Anchor {
    /// Create a new anchor.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            created: now,
            from: now,
            to: None,
        }
    }
    /// Update the anchor.
    pub fn update(self, now: DateTime<Utc>) -> Self {
        debug_assert!(self.to.is_none());
        Self {
            created: self.created,
            from: now,
            to: None,
        }
    }
    // /// Update the anchor, returning the previous (closed) anchor.
    // pub fn updated(&mut self, now: DateTime<Utc>) -> Self {
    //     debug_assert!(self.to.is_none());
    //     Self {
    //         created: self.created,
    //         from: std::mem::replace(&mut self.from, now),
    //         to: Some(now),
    //     }
    // }
    /// Close the anchor.
    pub fn remove(self, now: DateTime<Utc>) -> Self {
        Self {
            to: Some(now),
            ..self
        }
    }
    pub fn alive(&self, t: DateTime<Utc>) -> bool {
        self.from <= t && self.to.map_or(true, |to| to > t)
    }
}

impl TimeRange {
    pub fn new(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Self {
        Self { from, to }
    }
    pub fn between(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        Self::new(Some(from), Some(to))
    }
    pub fn at(time: DateTime<Utc>) -> Self {
        Self::between(time, time)
    }
    pub fn since(from: DateTime<Utc>) -> Self {
        Self::new(Some(from), None)
    }
    pub fn till(to: DateTime<Utc>) -> Self {
        Self::new(None, Some(to))
    }
    pub fn always() -> Self {
        Self::new(None, None)
    }
}

impl<T> Timestamped<T> {
    pub fn new(value: T, now: DateTime<Utc>) -> Self {
        Self {
            timestamp: now,
            value,
        }
    }
    pub fn schema_with(value: DbSchema) -> DbSchema {
        StructSchema::new()
            .field("timestamp", DateTimeSchema::new())
            .field("value", value)
            .flatten("value")
            .elastic_rename("timestamp", "@timestamp")
            .into()
    }
}

impl<T: HasSchema> HasSchema for Timestamped<T> {
    fn schema() -> DbSchema {
        Self::schema_with(T::schema())
    }
}

impl HasSchema1 for Timestamped<Value> {
    fn schema1(value: DbSchema) -> DbSchema {
        Self::schema_with(value)
    }
}

impl ObjectId {
    pub fn new() -> Self {
        Self(Cow::Owned(Uuid::new_v4().to_string()))
    }
    pub const fn from_static(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }
    pub fn from(id: String) -> Self {
        Self(Cow::Owned(id))
    }
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl HasSchema for ObjectId {
    fn schema() -> DbSchema {
        StringSchema::new().into()
    }
}

impl<T> Identified<T> {
    pub fn new(value: T) -> Self {
        Self::new_id(ObjectId::new(), value)
    }
    pub fn new_id(object_id: ObjectId, value: T) -> Self {
        Self { object_id, value }
    }
    pub fn schema_with(value: DbSchema) -> DbSchema {
        StructSchema::new()
            .field("object_id", StringSchema::new())
            .field("value", value)
            .flatten("value")
            .into()
    }
}

impl<T: HasSchema> HasSchema for Identified<T> {
    fn schema() -> DbSchema {
        Self::schema_with(T::schema())
    }
}

impl<T: HasSchema1> HasSchema1 for Identified<T> {
    fn schema1(value: DbSchema) -> DbSchema {
        Self::schema_with(T::schema1(value))
    }
}

impl<V, T> Versioned<V, T> {
    pub fn schema_with(version: DbSchema, value: DbSchema) -> DbSchema {
        StructSchema::new()
            .field("version", version)
            .field("value", value)
            .flatten("version")
            .flatten("value")
            .into()
    }
}

impl<V: HasSchema> HasSchema1 for Versioned<V, Value> {
    fn schema1(value: DbSchema) -> DbSchema {
        Self::schema_with(V::schema(), value)
    }
}

impl<V: HasSchema, T: HasSchema> HasSchema for Versioned<V, T> {
    fn schema() -> DbSchema {
        Self::schema_with(V::schema(), T::schema())
    }
}

impl HasSchema for SingleVersionInfo {
    fn schema() -> DbSchema {
        StructSchema::new()
            .field("active", Anchor::schema())
            .elastic_rename("active", "@active")
            .into()
    }
}

impl HasSchema for DualVersionInfo {
    fn schema() -> DbSchema {
        StructSchema::new()
            .field("current", Anchor::schema())
            .field("active", OptionSchema::new(Anchor::schema()))
            .field("committed", OptionSchema::new(DateTimeSchema::new()))
            .elastic_rename("current", "@current")
            .elastic_rename("active", "@active")
            .elastic_rename("committed", "@committed")
            .into()
    }
}

impl HasSchema for Anchor {
    fn schema() -> DbSchema {
        StructSchema::new()
            .field("from", DateTimeSchema::new())
            .field("to", OptionSchema::new(DateTimeSchema::new()))
            .field("created", DateTimeSchema::new())
            .into()
    }
}

impl<T: HasSchema> HasTableDef for Identified<Timestamped<T>> {
    fn table_def() -> DbTable {
        DbTable {
            versioning: VersioningType::Timestamped,
            force_update: false,
            schema: T::schema()
                .into_named_field_schema()
                .expect("table schema must be a struct"),
        }
    }
}

impl<T: HasSchema> HasTableDef for Identified<SingleVersioned<T>> {
    fn table_def() -> DbTable {
        DbTable {
            versioning: VersioningType::SingleTimeline,
            force_update: false,
            schema: T::schema()
                .into_named_field_schema()
                .expect("table schema must be a struct"),
        }
    }
}

impl<T: HasSchema> HasTableDef for Identified<DualVersioned<T>> {
    fn table_def() -> DbTable {
        DbTable {
            versioning: VersioningType::DualTimeline,
            force_update: false,
            schema: T::schema()
                .into_named_field_schema()
                .expect("table schema must be a struct"),
        }
    }
}

/* Note: this is used in phrases like "this operation is only available
 * on {} indices." */
impl std::fmt::Display for VersioningType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timestamped => write!(f, "timestamped"),
            Self::SingleTimeline => write!(f, "single-timeline"),
            Self::DualTimeline => write!(f, "dual-timeline"),
        }
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}
