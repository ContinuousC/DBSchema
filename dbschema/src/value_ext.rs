/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use super::error::{Error, Result};
use super::schema::EnumFormat;

/// Add a few helper functions to serde_json::Value.
pub trait ValueExt {
    /// Describe the type of a Json Value.
    fn value_type(&self) -> &'static str;

    fn as_object_or_err(&self) -> Result<&Map<String, Value>>;
    fn as_array_or_err(&self) -> Result<&Vec<Value>>;
    fn as_str_or_err(&self) -> Result<&str>;
    fn as_f64_or_err(&self) -> Result<f64>;
    fn as_i64_or_err(&self) -> Result<i64>;
    fn as_bool_or_err(&self) -> Result<bool>;
    fn as_null_or_err(&self) -> Result<()>;
    fn as_enum_or_err(&self, format: &EnumFormat) -> Result<(&String, &Value)>;
    fn as_date_time_or_err(&self) -> Result<DateTime<Utc>>;
}

impl ValueExt for Value {
    fn value_type(&self) -> &'static str {
        match self {
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::String(_) => "string",
            Value::Number(num) if num.is_f64() => "float",
            Value::Number(num) if num.is_i64() => "integer",
            Value::Number(_) => "number",
            Value::Bool(_) => "bool",
            Value::Null => "null",
        }
    }

    fn as_object_or_err(&self) -> Result<&Map<String, Value>> {
        self.as_object()
            .ok_or_else(|| Error::ValueTypeError("object", self.value_type()))
    }

    fn as_array_or_err(&self) -> Result<&Vec<Value>> {
        self.as_array()
            .ok_or_else(|| Error::ValueTypeError("array", self.value_type()))
    }

    fn as_str_or_err(&self) -> Result<&str> {
        self.as_str()
            .ok_or_else(|| Error::ValueTypeError("string", self.value_type()))
    }

    fn as_f64_or_err(&self) -> Result<f64> {
        self.as_f64()
            .or_else(|| self.as_i64().map(|v| v as f64))
            .or_else(|| self.as_u64().map(|v| v as f64))
            .ok_or_else(|| Error::ValueTypeError("float", self.value_type()))
    }

    fn as_i64_or_err(&self) -> Result<i64> {
        self.as_i64()
            .or_else(|| self.as_f64().map(|v| f64::round(v) as i64))
            .ok_or_else(|| Error::ValueTypeError("integer", self.value_type()))
    }

    fn as_bool_or_err(&self) -> Result<bool> {
        self.as_bool()
            .ok_or_else(|| Error::ValueTypeError("bool", self.value_type()))
    }

    fn as_null_or_err(&self) -> Result<()> {
        self.as_null()
            .ok_or_else(|| Error::ValueTypeError("null", self.value_type()))
    }

    fn as_enum_or_err(&self, format: &EnumFormat) -> Result<(&String, &Value)> {
        const NULL: Value = Value::Null;
        match self {
            Value::Object(map)
                if *format == EnumFormat::ExternallyTagged
                    || *format == EnumFormat::UnitAsString =>
            {
                match map.len() == 1 {
                    true => Ok(map.iter().next().unwrap()),
                    false => Err(Error::InvalidEnumValue),
                }
            }
            Value::String(s)
                if *format == EnumFormat::UnitAsString
                    || *format == EnumFormat::TagString =>
            {
                Ok((s, &NULL))
            }
            _ => Err(Error::InvalidEnumValue),
        }
    }

    fn as_date_time_or_err(&self) -> Result<DateTime<Utc>> {
        let s = self.as_str_or_err()?;
        Ok(DateTime::parse_from_rfc3339(s)
            .map_err(|e| Error::InvalidDateTime(s.to_string(), e))?
            .with_timezone(&Utc))
    }
}
