/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::collections::BTreeSet;

use syn::{Attribute, LitStr};

use crate::rename::SerdeRenameAll;

pub(crate) struct StructAttrs {
    pub(crate) removed: BTreeSet<String>,
    pub(crate) serde_rename_all: Option<SerdeRenameAll>,
}

pub(crate) struct EnumAttrs {
    pub(crate) format: Option<String>,
    pub(crate) serde_rename_all: Option<SerdeRenameAll>,
}

#[derive(Debug, Default)]
pub(crate) struct FieldAttrs {
    pub(crate) serde_rename: Option<String>,
    pub(crate) serde_default: Option<SerdeDefault>,
    pub(crate) serde_flatten: bool, // Flatten in in-memory JSON representation
    pub(crate) elastic_rename: Option<String>,
    pub(crate) argument: Option<String>,
    pub(crate) flatten: bool, // Flatten in database
    pub(crate) json: bool,    // Save as JSON unstructured value
}

#[derive(Debug, Default)]
pub(crate) struct VariantAttrs {
    pub(crate) removed: BTreeSet<String>,
    pub(crate) serde_rename_all: Option<SerdeRenameAll>,
    pub(crate) serde_rename: Option<String>,
}

#[derive(Debug)]
pub enum SerdeDefault {
    Default,
    Function(String),
}

impl From<&[Attribute]> for StructAttrs {
    fn from(attrs: &[Attribute]) -> Self {
        let mut removed = BTreeSet::new();
        let mut serde_rename_all = None;

        for attr in attrs {
            if attr.path().is_ident("dbschema") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("removed") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        removed.insert(s.value());
                        Ok(())
                    } else {
                        Err(meta.error("invalid dbschema attribute"))
                    }
                })
                .unwrap();
            } else if attr.path().is_ident("serde") {
                // We are in foreign territory: pick up tags
                // we are interested in and ignore others.
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename_all") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        match SerdeRenameAll::parse(&s.value()) {
                            Some(v) => serde_rename_all = Some(v),
                            None => Err(meta.error(
                                "unsupported value for serde(rename_all)",
                            ))?,
                        }
                    }

                    Ok(())
                })
                .unwrap();
            }
        }
        Self {
            removed,
            serde_rename_all,
        }
    }
}

impl From<&[Attribute]> for EnumAttrs {
    fn from(attrs: &[Attribute]) -> Self {
        let mut format = None;
        let mut serde_rename_all = None;

        for attr in attrs {
            if attr.path().is_ident("dbschema") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("format") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        format = Some(s.value().to_string());
                        Ok(())
                    } else {
                        Err(meta.error("invalid dbschema attribute"))
                    }
                })
                .unwrap();
            } else if attr.path().is_ident("serde") {
                // We are in foreign territory: pick up tags
                // we are interested in and ignore others.
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename_all") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        match SerdeRenameAll::parse(&s.value()) {
                            Some(v) => serde_rename_all = Some(v),
                            None => Err(meta.error(
                                "unsupported value for serde(rename_all)",
                            ))?,
                        }
                    }

                    Ok(())
                })
                .unwrap();
            }
        }
        Self {
            format,
            serde_rename_all,
        }
    }
}

impl From<&[Attribute]> for FieldAttrs {
    fn from(attrs: &[Attribute]) -> Self {
        let mut serde_rename = None;
        let mut serde_default = None;
        let mut serde_flatten = false;
        let mut elastic_rename = None;
        let mut argument = None;
        let mut flatten = false;
        let mut json = false;
        for attr in attrs {
            if attr.path().is_ident("dbschema") {
                // This is our territory: panic for anything unexpected.
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("json") {
                        json = true;
                        Ok(())
                    } else if meta.path.is_ident("flatten") {
                        flatten = true;
                        Ok(())
                    } else if meta.path.is_ident("elastic_rename") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        elastic_rename = Some(s.value());
                        Ok(())
                    } else if meta.path.is_ident("argument") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        argument = Some(s.value());
                        Ok(())
                    } else {
                        Err(meta.error("unknown dbschema field attribute"))
                    }
                })
                .unwrap();
            } else if attr.path().is_ident("serde") {
                // We are in foreign territory: pick up tags
                // we are interested in and ignore others.
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        serde_rename = Some(s.value());
                    } else if meta.path.is_ident("flatten") {
                        serde_flatten = true;
                    } else if meta.path.is_ident("default") {
                        if let Ok(s) = meta
                            .input
                            .parse::<syn::token::Eq>()
                            .and_then(|_| meta.input.parse::<LitStr>())
                        {
                            serde_default =
                                Some(SerdeDefault::Function(s.value()));
                        } else {
                            serde_default = Some(SerdeDefault::Default);
                        }
                    }

                    Ok(())
                })
                .unwrap();
            }
        }
        Self {
            serde_rename,
            serde_default,
            serde_flatten,
            elastic_rename,
            argument,
            flatten,
            json,
        }
    }
}

impl From<&[Attribute]> for VariantAttrs {
    fn from(attrs: &[Attribute]) -> Self {
        let mut removed = BTreeSet::new();
        let mut serde_rename = None;
        let mut serde_rename_all = None;
        for attr in attrs {
            if attr.path().is_ident("dbschema") {
                // This is our territory: panic for anything unexpected.
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("removed") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        removed.insert(s.value());
                        Ok(())
                    } else {
                        Err(meta.error("invalid dbschema variant attribute"))
                    }
                })
                .unwrap();
            } else if attr.path().is_ident("serde") {
                // We are in foreign territory: pick up tags
                // we are interested in and ignore others.
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename_all") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s: LitStr = meta.input.parse()?;
                        match SerdeRenameAll::parse(&s.value()) {
                            Some(v) => {
                                serde_rename_all = Some(v);
                            }
                            None => Err(meta.error(
                                "unsupported value for serde::rename_all",
                            ))?,
                        }
                    } else if meta.path.is_ident("rename") {
                        let _: syn::token::Eq = meta.input.parse()?;
                        let s = meta.input.parse::<LitStr>()?;
                        serde_rename = Some(s.value());
                    }

                    Ok(())
                })
                .unwrap();
            }
        }
        Self {
            serde_rename_all,
            serde_rename,
            removed,
        }
    }
}
