/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::{cmp::Ordering, ops::BitAnd};

use serde::Serialize;
use serde_json::{json, Value};

use super::filter::EsFilter;

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Range<'a, T>(
    pub(crate) Vec<&'a str>,
    pub(crate) RangeLb<T>,
    pub(crate) RangeUb<T>,
);

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) enum RangeLb<T> {
    Free,
    Ge(T),
    Gt(T),
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) enum RangeUb<T> {
    Free,
    Le(T),
    Lt(T),
}

impl<'a, T: Serialize> Range<'a, T> {
    pub(crate) fn to_value(&self) -> Value {
        let bounds = Value::Object(
            IntoIterator::into_iter([
                match &self.1 {
                    RangeLb::Free => None,
                    RangeLb::Ge(val) => Some(("gte".to_string(), json!(val))),
                    RangeLb::Gt(val) => Some(("gt".to_string(), json!(val))),
                },
                match &self.2 {
                    RangeUb::Free => None,
                    RangeUb::Le(val) => Some(("lte".to_string(), json!(val))),
                    RangeUb::Lt(val) => Some(("lt".to_string(), json!(val))),
                },
            ])
            .flatten()
            .collect(),
        );
        json!({"range": {self.0.join("."): bounds}})
    }
}

impl<'a, T: PartialOrd + Clone> Range<'a, T> {
    pub(crate) fn simplify<F>(&self, f: F) -> EsFilter<'a>
    where
        F: FnOnce(Range<'a, T>) -> EsFilter<'a>,
    {
        match (&self.1, &self.2) {
            (RangeLb::Free, RangeUb::Free) => EsFilter::Always,
            (RangeLb::Free, _) => f(self.clone()),
            (_, RangeUb::Free) => f(self.clone()),
            (RangeLb::Ge(a), RangeUb::Le(b)) => match a > b {
                true => EsFilter::Never,
                false => f(self.clone()),
            },
            (RangeLb::Ge(a), RangeUb::Lt(b))
            | (RangeLb::Gt(a), RangeUb::Le(b))
            | (RangeLb::Gt(a), RangeUb::Lt(b)) => match a >= b {
                true => EsFilter::Never,
                false => f(self.clone()),
            },
        }
    }
}

// fn join_ranges<'a, T, F, G>(
//     seen: BTreeSet<Vec<&'a str>>,
//     filters: &[EsFilter<'a>],
//     range: Range<'a, T>,
//     extract: F,
//     inject: G,
// ) -> Option<EsFilter<'a>>
// where
//     F: Fn(EsFilter<'a>) -> Option<Range<'a, T>>,
//     G: Fn(Range<'a, T>) -> EsFilter<'a>,
// {
//     match seen.insert(range.0.to_vec()) {
//         true => Some(
//             filters
//                 .iter()
//                 .fold(range, |range, filter| match filter {
//                     EsFilter::IntegerRange(r) if r.0 == range.0 => {
//                         range & r.clone()
//                     }
//                     _ => range,
//                 })
//                 .simplify(EsFilter::IntegerRange),
//         ),
//         false => None,
//     }
// }

// fn range_from_schema<'a, LB, UB>(
//     schema: &'a DbSchema,
//     root: &[&'a str],
//     value: Value,
// ) -> Result<EsFilter<'a>, FilterError> {
//     match schema {
//         DbSchema::Double(_) => Ok(EsFilter::DoubleRange(Range::gt(
//             root.to_vec(),
//             value.as_f64_or_err()?,
//         ))),
//         DbSchema::Integer(_) => Ok(EsFilter::IntegerRange(Range::gt(
//             root.to_vec(),
//             value.as_i64_or_err()?,
//         ))),
//         DbSchema::DateTime(_) => Ok(EsFilter::DateTimeRange(Range::gt(
//             root.to_vec(),
//             value.as_date_time_or_err()?,
//         ))),
//         _ => Err(FilterError::NotOrderable(schema.kind())),
//     }
// }

impl<'a, T: PartialOrd> BitAnd for Range<'a, T> {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self(
            self.0,
            match rhs.1 > self.1 {
                false => self.1,
                true => rhs.1,
            },
            match rhs.2 < self.2 {
                false => self.2,
                true => rhs.2,
            },
        )
    }
}

impl<T: PartialOrd> PartialOrd for RangeLb<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Free, Self::Free) => Some(Ordering::Equal),
            (Self::Free, _) => Some(Ordering::Less),
            (_, Self::Free) => Some(Ordering::Greater),
            (Self::Ge(a), Self::Ge(b)) => a.partial_cmp(b),
            (Self::Gt(a), Self::Gt(b)) => a.partial_cmp(b),
            (Self::Ge(a), Self::Gt(b)) => match a > b {
                true => Some(Ordering::Greater),
                false => Some(Ordering::Less),
            },
            (Self::Gt(a), Self::Ge(b)) => match a < b {
                true => Some(Ordering::Less),
                false => Some(Ordering::Greater),
            },
        }
    }
}

impl<T: PartialOrd> PartialOrd for RangeUb<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Free, Self::Free) => Some(Ordering::Equal),
            (Self::Free, _) => Some(Ordering::Greater),
            (_, Self::Free) => Some(Ordering::Less),
            (Self::Le(a), Self::Le(b)) => a.partial_cmp(b),
            (Self::Lt(a), Self::Lt(b)) => a.partial_cmp(b),
            (Self::Le(a), Self::Lt(b)) => match a < b {
                true => Some(Ordering::Less),
                false => Some(Ordering::Greater),
            },
            (Self::Lt(a), Self::Le(b)) => match a > b {
                true => Some(Ordering::Greater),
                false => Some(Ordering::Less),
            },
        }
    }
}
