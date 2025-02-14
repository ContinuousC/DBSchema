/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fmt;
use std::marker::PhantomData;

use serde::de::{Deserializer, Error, MapAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};

/// A structure with serialized representation { K: V }. The key
/// type must therefore serialize as a string.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct Tagged<K, V> {
    pub tag: K,
    pub value: V,
}

impl<K, V> Serialize for Tagged<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.tag, &self.value)?;
        map.end()
    }
}

impl<'de, K, V> Deserialize<'de> for Tagged<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(MapVisitor(PhantomData))
    }
}

struct MapVisitor<K, V>(PhantomData<(K, V)>);

impl<'de, K, V> Visitor<'de> for MapVisitor<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    type Value = Tagged<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "A map with exactly one entry.")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        match map.next_entry()? {
            Some((tag, value)) => match map.next_entry::<K, V>()?.is_some() {
                false => Ok(Tagged { tag, value }),
                true => Err(A::Error::invalid_length(2, &self)),
            },
            None => Err(A::Error::invalid_length(0, &self)),
        }
    }
}
