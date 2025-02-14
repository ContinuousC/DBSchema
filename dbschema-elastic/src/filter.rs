/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::collections::BTreeSet;
use std::fmt;
use std::iter::once;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use dbschema::{
    At, DictionarySchema, Filter, FilterPath, ListSchema, PathElem,
};
use dbschema::{DbSchema, EnumSchema, OptionSchema, SetSchema, StructSchema};
use dbschema::{EnumFormat, ValueExt};

use super::error::FilterError;
use super::range::{Range, RangeLb, RangeUb};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(transparent)]
pub struct ElasticFilter(Value);

impl ElasticFilter {
    pub fn new(
        schema: &DbSchema,
        filter: &Filter,
    ) -> Result<Self, FilterError> {
        let intermediate = EsFilter::build_filter(schema, filter, &[])?;
        let simplified = intermediate.simplify();
        Ok(Self(simplified.to_value()))
    }
}

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum EsFilter<'a> {
    /// { "match_all": {} }
    Always,
    /// { "match_none": {} }
    Never,
    /// { "bool": { "must": [ filters ] } } --> filter1 && filter2 && ...
    All(Vec<EsFilter<'a>>),
    /// { "bool": { "should": [ filters ] } } --> filter1 || filter2 || ...
    Any(Vec<EsFilter<'a>>),
    /// { "bool": { "must_not": filter } } --> !filter
    Not(Box<EsFilter<'a>>),
    /// { "term": { path: value } }
    Term(Vec<&'a str>, Value),
    /// { "terms": { path: value } }
    Terms(Vec<&'a str>, Vec<Value>),
    /// { "exists": path }
    Exists(Vec<&'a str>),
    /// { "range": { path: { lb, ub }} }
    IntegerRange(Range<'a, i64>),
    DoubleRange(Range<'a, f64>),
    DateTimeRange(Range<'a, DateTime<Utc>>),
    /// Used to indicate queries on lists or dictionaries.
    /// This is implicit in elastic, but we need to know
    /// this for simplification.
    Iter(Box<EsFilter<'a>>),
}

impl<'a> EsFilter<'a> {
    fn build_filter<'b>(
        schema: &'a DbSchema,
        filter: &'b Filter,
        root: &'b [&'a str],
    ) -> Result<EsFilter<'a>, FilterError> {
        match filter {
            Filter::All(filters) => {
                let filters = filters
                    .iter()
                    .map(|filter| Self::build_filter(schema, filter, root))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(EsFilter::All(filters))
            }
            Filter::Any(filters) => {
                let filters = filters
                    .iter()
                    .map(|filter| Self::build_filter(schema, filter, root))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(EsFilter::Any(filters))
            }
            Filter::Not(filter) => {
                let filter = Self::build_filter(schema, filter, root)?;
                Ok(EsFilter::Not(Box::new(filter)))
            }
            Filter::At(At { path, filter, .. }) => {
                Self::build_path(schema, filter, root, &path.0)
            }
            Filter::AllElems(filter) | Filter::AnyElem(filter) => {
                /* Elastic will always match "any", so further
                 * filtering in memory is necessary. */
                match schema {
                    DbSchema::List(ListSchema { value_type, .. }) => {
                        Self::build_filter(value_type, filter, root)
                    }
                    DbSchema::Set(SetSchema { value_type, .. }) => {
                        Self::build_filter(value_type, filter, root)
                    }
                    DbSchema::Dictionary(DictionarySchema {
                        value_type,
                        ..
                    }) => {
                        let root: Vec<_> =
                            root.iter().cloned().chain(once("value")).collect();
                        Ok(EsFilter::Iter(Box::new(Self::build_filter(
                            value_type, filter, &root,
                        )?)))
                    }
                    _ => Err(FilterError::NotIterable(schema.kind())),
                }
            }
            Filter::In(values) => match schema {
                DbSchema::String(_) => {
                    let path: Vec<_> =
                        root.iter().cloned().chain(once("keyword")).collect();
                    Ok(EsFilter::Terms(path.to_vec(), values.to_vec()))
                }
                DbSchema::Ipv4(_)
                | DbSchema::Ipv6(_)
                | DbSchema::DateTime(_)
                | DbSchema::Integer(_)
                | DbSchema::Double(_)
                | DbSchema::Bool(_) => {
                    Ok(EsFilter::Terms(root.to_vec(), values.clone()))
                }
                DbSchema::Unit(_) => match values.is_empty() {
                    true => Ok(EsFilter::Never),
                    false => Ok(EsFilter::Always),
                },
                _ => Err(FilterError::NotInComparable(schema.kind())),
            },
            Filter::NotIn(values) => Ok(EsFilter::Not(Box::new(
                Self::build_filter(schema, &Filter::In(values.to_vec()), root)?,
            ))),
            Filter::Eq(value) => match schema {
                DbSchema::Struct(StructSchema { fields, .. }) => {
                    /* TODO: non-default flatten, elastic_rename */
                    let map =
                        value.as_object().ok_or(FilterError::InvalidValue)?;
                    let filters = fields
                        .iter()
                        .map(|(field, field_schema)| {
                            let field_value = match field_schema {
                                DbSchema::Option(_) => map
                                    .get(field)
                                    .cloned()
                                    .unwrap_or(Value::Null),
                                _ => map
                                    .get(field)
                                    .ok_or(FilterError::InvalidValue)?
                                    .clone(),
                            };
                            let root: Vec<_> = root
                                .iter()
                                .cloned()
                                .chain(once(field.as_str()))
                                .collect();
                            Self::build_filter(
                                field_schema,
                                &Filter::Eq(field_value),
                                &root,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(EsFilter::All(filters))
                }
                DbSchema::Dictionary(_) => {
                    let map =
                        value.as_object().ok_or(FilterError::InvalidValue)?;
                    let path: Vec<_> =
                        root.iter().cloned().chain(once("value")).collect();
                    let exists = EsFilter::Exists(path.to_vec());
                    match map.is_empty() {
                        true => Ok(EsFilter::Not(Box::new(exists))),
                        false => Ok(exists), /* Further filtering in memory. */
                    }
                }
                DbSchema::List(_) | DbSchema::Set(_) => {
                    let map =
                        value.as_object().ok_or(FilterError::InvalidValue)?;
                    let exists = EsFilter::Exists(root.to_vec());
                    match map.is_empty() {
                        true => Ok(EsFilter::Not(Box::new(exists))),
                        false => Ok(exists), /* Further filtering in memory. */
                    }
                }
                DbSchema::Option(OptionSchema { value_type, .. }) => {
                    match value {
                        Value::Null => Ok(EsFilter::Not(Box::new(
                            EsFilter::Exists(root.to_vec()),
                        ))),
                        _ => Ok(EsFilter::All(vec![
                            EsFilter::Exists(root.to_vec()),
                            Self::build_filter(value_type, filter, root)?,
                        ])),
                    }
                }
                DbSchema::Enum(EnumSchema {
                    options, format, ..
                }) => {
                    /* TODO: eq on non-externally tagged enums */
                    let (tag, value) = value.as_enum_or_err(format)?;
                    let (option_key, option_schema) =
                        options.get_key_value(tag).ok_or_else(|| {
                            FilterError::MissingOption(tag.to_string())
                        })?;
                    match format {
                        EnumFormat::TagString => {
                            Ok(EsFilter::Term(root.to_vec(), json!(option_key)))
                        }
                        _ => {
                            let path: Vec<_> = root
                                .iter()
                                .cloned()
                                .chain(once(option_key.as_str()))
                                .collect();
                            Ok(EsFilter::All(vec![
                                EsFilter::Exists(path.to_vec()),
                                Self::build_filter(
                                    option_schema,
                                    &Filter::Eq(value.clone()),
                                    &path,
                                )?,
                            ]))
                        }
                    }
                }
                //DbSchema::Json(_) => Ok(EsFilter::Always), /* Can json be matched? */
                DbSchema::String(_) => {
                    let path: Vec<_> =
                        root.iter().cloned().chain(once("keyword")).collect();
                    Ok(EsFilter::Term(path.to_vec(), value.clone()))
                }
                DbSchema::Ipv4(_)
                | DbSchema::Ipv6(_)
                | DbSchema::DateTime(_)
                | DbSchema::Integer(_)
                | DbSchema::Double(_)
                | DbSchema::Bool(_) => {
                    Ok(EsFilter::Term(root.to_vec(), value.clone()))
                }
                DbSchema::Unit(_) => Ok(EsFilter::Always),
                _ => Err(FilterError::NotComparable(schema.kind())),
            },
            Filter::Ne(value) => Ok(EsFilter::Not(Box::new(
                Self::build_filter(schema, &Filter::Eq(value.clone()), root)?,
            ))),
            Filter::Gt(value) => match schema {
                DbSchema::Double(_) => Ok(EsFilter::DoubleRange(Range(
                    root.to_vec(),
                    RangeLb::Gt(value.as_f64_or_err()?),
                    RangeUb::Free,
                ))),
                DbSchema::Integer(_) => Ok(EsFilter::IntegerRange(Range(
                    root.to_vec(),
                    RangeLb::Gt(value.as_i64_or_err()?),
                    RangeUb::Free,
                ))),
                DbSchema::DateTime(_) => Ok(EsFilter::DateTimeRange(Range(
                    root.to_vec(),
                    RangeLb::Gt(value.as_date_time_or_err()?),
                    RangeUb::Free,
                ))),
                _ => Err(FilterError::NotOrderable(schema.kind())),
            },
            Filter::Ge(value) => match schema {
                DbSchema::Double(_) => Ok(EsFilter::DoubleRange(Range(
                    root.to_vec(),
                    RangeLb::Ge(value.as_f64_or_err()?),
                    RangeUb::Free,
                ))),
                DbSchema::Integer(_) => Ok(EsFilter::IntegerRange(Range(
                    root.to_vec(),
                    RangeLb::Ge(value.as_i64_or_err()?),
                    RangeUb::Free,
                ))),
                DbSchema::DateTime(_) => Ok(EsFilter::DateTimeRange(Range(
                    root.to_vec(),
                    RangeLb::Ge(value.as_date_time_or_err()?),
                    RangeUb::Free,
                ))),
                _ => Err(FilterError::NotOrderable(schema.kind())),
            },
            Filter::Le(value) => match schema {
                DbSchema::Double(_) => Ok(EsFilter::DoubleRange(Range(
                    root.to_vec(),
                    RangeLb::Free,
                    RangeUb::Le(value.as_f64_or_err()?),
                ))),
                DbSchema::Integer(_) => Ok(EsFilter::IntegerRange(Range(
                    root.to_vec(),
                    RangeLb::Free,
                    RangeUb::Le(value.as_i64_or_err()?),
                ))),
                DbSchema::DateTime(_) => Ok(EsFilter::DateTimeRange(Range(
                    root.to_vec(),
                    RangeLb::Free,
                    RangeUb::Le(value.as_date_time_or_err()?),
                ))),
                _ => Err(FilterError::NotOrderable(schema.kind())),
            },
            Filter::Lt(value) => match schema {
                DbSchema::Double(_) => Ok(EsFilter::DoubleRange(Range(
                    root.to_vec(),
                    RangeLb::Free,
                    RangeUb::Lt(value.as_f64_or_err()?),
                ))),
                DbSchema::Integer(_) => Ok(EsFilter::IntegerRange(Range(
                    root.to_vec(),
                    RangeLb::Free,
                    RangeUb::Lt(value.as_i64_or_err()?),
                ))),
                DbSchema::DateTime(_) => Ok(EsFilter::DateTimeRange(Range(
                    root.to_vec(),
                    RangeLb::Free,
                    RangeUb::Lt(value.as_date_time_or_err()?),
                ))),
                _ => Err(FilterError::NotOrderable(schema.kind())),
            },
        }
    }

    fn build_path<'b>(
        schema: &'a DbSchema,
        filter: &'b Filter,
        root: &'b [&'a str],
        path: &'b [PathElem],
    ) -> Result<EsFilter<'a>, FilterError> {
        match path.is_empty() {
            true => Self::build_filter(schema, filter, root),
            false => match &path[0] {
                PathElem::Field(field) => match schema {
                    DbSchema::Struct(StructSchema {
                        fields,
                        flatten,
                        elastic_rename,
                        ..
                    }) => {
                        let (field_key, field_schema) =
                            fields.get_key_value(field).ok_or_else(|| {
                                FilterError::MissingField(field.to_string())
                            })?;
                        let root: Vec<&str> = match flatten.contains(field) {
                            true => root.to_vec(),
                            false => root
                                .iter()
                                .cloned()
                                .chain(once(
                                    elastic_rename
                                        .get(field_key)
                                        .unwrap_or(field_key)
                                        .as_str(),
                                ))
                                .collect(),
                        };
                        let esfilter = Self::build_path(
                            field_schema,
                            filter,
                            &root,
                            &path[1..],
                        )?;
                        match field_schema.default() {
                            Some(default) => {
                                let exist = EsFilter::Exists(root.to_vec());
                                let path = FilterPath(path[1..].to_vec());
                                let (sub_schema, sub_value) =
                                    path.walk_value(field_schema, &default)?;
                                match sub_value.map_or(
                                    Ok(false),
                                    |sub_value| {
                                        filter.matches(sub_schema, sub_value)
                                    },
                                )? {
                                    true => {
                                        let not_exist =
                                            EsFilter::Not(Box::new(exist));
                                        Ok(EsFilter::Any(vec![
                                            not_exist, esfilter,
                                        ]))
                                    }
                                    false => {
                                        Ok(EsFilter::All(vec![exist, esfilter]))
                                    }
                                }
                            }
                            None => Ok(esfilter),
                        }
                    }
                    _ => {
                        Err(FilterError::InvalidSchema("struct", schema.kind()))
                    }
                },
                PathElem::Option(option) => match schema {
                    DbSchema::Enum(EnumSchema {
                        options, format, ..
                    }) => {
                        let (option_key, option_schema) =
                            options.get_key_value(option).ok_or_else(|| {
                                FilterError::MissingOption(option.to_string())
                            })?;
                        match format {
                            EnumFormat::ExternallyTagged
                            | EnumFormat::UnitAsString => {
                                let root: Vec<&str> = root
                                    .iter()
                                    .cloned()
                                    .chain(once(option_key.as_str()))
                                    .collect();
                                let esfilter = Self::build_path(
                                    option_schema,
                                    filter,
                                    &root,
                                    &path[1..],
                                )?;
                                let exist = EsFilter::Exists(root.to_vec());
                                Ok(EsFilter::All(vec![exist, esfilter]))
                            }
                            EnumFormat::TagString => {
                                let esfilter = Self::build_path(
                                    option_schema,
                                    filter,
                                    root,
                                    &path[1..],
                                )?;
                                let eq = EsFilter::Term(
                                    root.to_vec(),
                                    json!(option_key),
                                );
                                Ok(EsFilter::All(vec![eq, esfilter]))
                            }
                        }
                    }
                    _ => Err(FilterError::InvalidSchema("enum", schema.kind())),
                },
                PathElem::Some => match schema {
                    DbSchema::Option(OptionSchema { value_type, .. }) => {
                        let esfilter = Self::build_path(
                            value_type,
                            filter,
                            root,
                            &path[1..],
                        )?;
                        let exist = EsFilter::Exists(root.to_vec());
                        Ok(EsFilter::All(vec![exist, esfilter]))
                    }
                    _ => {
                        Err(FilterError::InvalidSchema("option", schema.kind()))
                    }
                },
            },
        }
    }

    fn simplify(&self) -> Self {
        match self {
            EsFilter::Always => EsFilter::Always,
            EsFilter::Never => EsFilter::Never,
            EsFilter::All(filters) => {
                let filters: Vec<_> = filters
                    .iter()
                    .map(Self::simplify)
                    .flat_map(|filter| match filter {
                        EsFilter::All(filters) => filters,
                        _ => vec![filter],
                    })
                    .collect();
                let mut seen_ranges = BTreeSet::new();
                let filters: Vec<_> = filters
                    .iter()
                    .cloned()
                    .filter_map(|filter| match filter {
                        EsFilter::IntegerRange(range) => match seen_ranges
                            .insert(range.0.to_vec())
                        {
                            true => Some(
                                filters
                                    .iter()
                                    .fold(range, |range, filter| match filter {
                                        EsFilter::IntegerRange(r)
                                            if r.0 == range.0 =>
                                        {
                                            range & r.clone()
                                        }
                                        _ => range,
                                    })
                                    .simplify(EsFilter::IntegerRange),
                            ),
                            false => None,
                        },
                        EsFilter::DoubleRange(range) => {
                            match seen_ranges.insert(range.0.to_vec()) {
                                true => Some(
                                    filters
                                        .iter()
                                        .fold(range, |range, filter| {
                                            match filter {
                                                EsFilter::DoubleRange(r)
                                                    if r.0 == range.0 =>
                                                {
                                                    range & r.clone()
                                                }
                                                _ => range,
                                            }
                                        })
                                        .simplify(EsFilter::DoubleRange),
                                ),
                                false => None,
                            }
                        }
                        EsFilter::DateTimeRange(range) => {
                            match seen_ranges.insert(range.0.to_vec()) {
                                true => Some(
                                    filters
                                        .iter()
                                        .fold(range, |range, filter| {
                                            match filter {
                                                EsFilter::DateTimeRange(r)
                                                    if r.0 == range.0 =>
                                                {
                                                    range & r.clone()
                                                }
                                                _ => range,
                                            }
                                        })
                                        .simplify(EsFilter::DateTimeRange),
                                ),
                                false => None,
                            }
                        }
                        _ => Some(filter),
                    })
                    .collect();
                let filters = filters
                    .iter()
                    .filter(|filter| *filter != &EsFilter::Always)
                    .cloned()
                    .fold(Vec::new(), |mut filters, filter| {
                        if !filters.contains(&filter) {
                            filters.push(filter);
                        }
                        filters
                    });
                let filters: Vec<_> = filters
                    .iter()
                    .filter(|filter| match filter {
                        EsFilter::Exists(path) => !filters.iter().any(|cmp| {
                            cmp != *filter && cmp.implies_existence(path)
                        }),
                        _ => true,
                    })
                    .cloned()
                    .collect();
                match filters.contains(&EsFilter::Never) {
                    true => EsFilter::Never,
                    false => match filters.len() {
                        0 => EsFilter::Always,
                        1 => filters.into_iter().next().unwrap(),
                        _ => EsFilter::All(filters),
                    },
                }
            }
            EsFilter::Any(filters) => {
                let filters: Vec<_> = filters
                    .iter()
                    .map(Self::simplify)
                    .flat_map(|filter| match filter {
                        EsFilter::Any(filters) => filters,
                        _ => vec![filter],
                    })
                    .filter(|filter| filter != &EsFilter::Never)
                    .fold(Vec::new(), |mut filters, filter| {
                        if !filters.contains(&filter) {
                            filters.push(filter);
                        }
                        filters
                    });
                match filters.contains(&EsFilter::Always) {
                    true => EsFilter::Always,
                    false => match filters.len() {
                        0 => EsFilter::Never,
                        1 => filters.into_iter().next().unwrap(),
                        _ => EsFilter::Any(filters),
                    },
                }
            }
            EsFilter::Not(filter) => match filter.simplify() {
                EsFilter::Always => EsFilter::Never,
                EsFilter::Never => EsFilter::Always,
                EsFilter::Not(filter) => *filter,
                EsFilter::All(filters) => EsFilter::Any(
                    filters
                        .into_iter()
                        .map(|filter| {
                            EsFilter::Not(Box::new(filter)).simplify()
                        })
                        .collect(),
                ),
                EsFilter::Any(filters) => EsFilter::All(
                    filters
                        .into_iter()
                        .map(|filter| {
                            EsFilter::Not(Box::new(filter)).simplify()
                        })
                        .collect(),
                ),
                filter => EsFilter::Not(Box::new(filter)),
            },
            EsFilter::IntegerRange(range) => {
                range.simplify(EsFilter::IntegerRange)
            }
            EsFilter::DoubleRange(range) => {
                range.simplify(EsFilter::DoubleRange)
            }
            EsFilter::DateTimeRange(range) => {
                range.simplify(EsFilter::DateTimeRange)
            }
            _ => self.clone(),
        }
    }

    fn implies_existence(&self, path: &[&'a str]) -> bool {
        match self {
            EsFilter::Always => false,
            EsFilter::Never => false,
            EsFilter::All(filters) => {
                filters.iter().any(|filter| filter.implies_existence(path))
            }
            EsFilter::Any(filters) => {
                !filters.is_empty()
                    && filters
                        .iter()
                        .all(|filter| filter.implies_existence(path))
            }
            EsFilter::Not(_) => false,
            EsFilter::Term(cmp, _)
            | EsFilter::Terms(cmp, _)
            | EsFilter::Exists(cmp)
            | EsFilter::IntegerRange(Range(cmp, _, _))
            | EsFilter::DoubleRange(Range(cmp, _, _))
            | EsFilter::DateTimeRange(Range(cmp, _, _)) => {
                cmp.starts_with(path)
            }
            EsFilter::Iter(filter) => filter.implies_existence(path),
        }
    }

    fn to_value(&self) -> Value {
        match self {
            EsFilter::Always => json!({"match_all": {}}),
            EsFilter::Never => json!({"match_none": {}}),
            EsFilter::All(filters) => {
                let filters: Vec<_> =
                    filters.iter().map(Self::to_value).collect();
                json!({ "bool": { "must": filters } })
            }
            EsFilter::Any(filters) => {
                let filters: Vec<_> =
                    filters.iter().map(Self::to_value).collect();
                json!({ "bool": { "should": filters } })
            }
            EsFilter::Not(filter) => {
                let filter = filter.to_value();
                json!({ "bool": { "must_not": filter } })
            }
            EsFilter::Iter(filter) => filter.to_value(),
            EsFilter::Term(path, value) => {
                json!({"term": {path.join("."): value}})
            }
            EsFilter::Terms(path, values) => {
                json!({"terms": {path.join("."): values}})
            }
            EsFilter::IntegerRange(range) => range.to_value(),
            EsFilter::DoubleRange(range) => range.to_value(),
            EsFilter::DateTimeRange(range) => range.to_value(),
            EsFilter::Exists(path) => json!({
                "exists": { "field": path.join(".") }
            }),
        }
    }
}

impl fmt::Display for ElasticFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string_pretty(&self.0).map_err(|_| fmt::Error)?
        )
    }
}
