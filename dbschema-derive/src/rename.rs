/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fmt::Write;

use syn::{ext::IdentExt, Ident};

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub(crate) enum SerdeRenameAll {
    LowerCase,
    UpperCase,
    SnakeCase,
    PascalCase,
    CamelCase,
}

impl SerdeRenameAll {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            //}s.value().as_str() {
            "lowercase" => Some(Self::LowerCase),
            "UPPERCASE" => Some(Self::UpperCase),
            "camelCase" => Some(Self::CamelCase),
            "PascalCase" => Some(Self::PascalCase),
            "snake_case" => Some(Self::SnakeCase),
            _ => None,
        }
    }

    pub(crate) fn rename(&self, id: &Ident) -> String {
        match self {
            Self::LowerCase => id.unraw().to_string().to_lowercase(),
            Self::UpperCase => id.unraw().to_string().to_uppercase(),
            Self::SnakeCase => snake_case(id.unraw().to_string()),
            Self::PascalCase => pascal_case(id.unraw().to_string()),
            Self::CamelCase => camel_case(id.unraw().to_string()),
        }
    }
}

pub(crate) fn rename(
    id: &Ident,
    rename: Option<&String>,
    rename_all: Option<&SerdeRenameAll>,
) -> String {
    match rename {
        Some(id) => id.to_string(),
        None => match rename_all {
            Some(r) => r.rename(id),
            None => id.unraw().to_string(),
        },
    }
}

fn snake_case(input: String) -> String {
    let mut output = String::new();
    for (i, c) in input.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            write!(output, "_").unwrap();
        }
        write!(output, "{}", c.to_lowercase()).unwrap();
    }
    output
}

fn pascal_case(input: String) -> String {
    let mut output = String::new();
    let mut first = true;
    for c in input.chars() {
        if c == '_' {
            first = true;
        } else if first {
            first = false;
            write!(output, "{}", c.to_uppercase()).unwrap();
        } else {
            write!(output, "{c}").unwrap();
        }
    }
    output
}

fn camel_case(input: String) -> String {
    let mut output = String::new();
    let mut first = false;
    for (i, c) in input.chars().enumerate() {
        if c == '_' {
            first = true;
        } else if first {
            first = false;
            write!(output, "{}", c.to_uppercase()).unwrap();
        } else if i == 0 {
            write!(output, "{}", c.to_lowercase()).unwrap();
        } else {
            write!(output, "{c}").unwrap();
        }
    }
    output
}
