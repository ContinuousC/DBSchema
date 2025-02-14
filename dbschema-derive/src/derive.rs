/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::collections::BTreeSet;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse_quote, Data, DataEnum, DataStruct, DeriveInput, Field, Fields,
    FieldsNamed, FieldsUnnamed, Ident,
};

use crate::{
    attributes::{
        EnumAttrs, FieldAttrs, SerdeDefault, StructAttrs, VariantAttrs,
    },
    rename::{rename, SerdeRenameAll},
};

pub(crate) fn impl_has_schema(
    nargs: usize,
    DeriveInput {
        ident,
        mut generics,
        data,
        attrs,
        ..
    }: DeriveInput,
) -> TokenStream {
    generics
        .type_params_mut()
        .for_each(|tp| tp.bounds.insert(0, parse_quote!(dbschema::HasSchema)));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let trait_name = Ident::new(
        &match nargs {
            0 => "HasSchema".to_string(),
            n => format!("HasSchema{n}"),
        },
        Span::call_site(),
    );
    let method_name = Ident::new(
        &match nargs {
            0 => "schema".to_string(),
            n => format!("schema{n}"),
        },
        Span::call_site(),
    );

    let schema = match &data {
        Data::Struct(DataStruct { fields, .. }) => {
            let struct_attrs = StructAttrs::from(attrs.as_ref());
            fields_schema(
                fields,
                &struct_attrs.serde_rename_all,
                &struct_attrs.removed,
            )
        }
        Data::Enum(DataEnum { variants, .. }) => {
            let enum_attrs = EnumAttrs::from(attrs.as_ref());
            let options = variants
                .iter()
                .map(|variant| {
                    let variant_attrs =
                        VariantAttrs::from(variant.attrs.as_ref());
                    let id = rename(
                        &variant.ident,
                        variant_attrs.serde_rename.as_ref(),
                        enum_attrs.serde_rename_all.as_ref(),
                    );
                    let schema = fields_schema(
                        &variant.fields,
                        &variant_attrs.serde_rename_all,
                        &variant_attrs.removed,
                    );
                    quote! { .option(#id, #schema) }
                })
                .collect::<Vec<_>>();
            let format = match enum_attrs
                .format
                .as_deref()
                .unwrap_or("unit_as_string")
            {
                "unit_as_string" => quote! {.unit_as_string()},
                "tag_string" => quote! {.tag_string()},
                s => panic!("invalid unit format: {}", s),
            };
            quote! {
                dbschema::EnumSchema::new()
                    #(#options)*#format
            }
        }
        Data::Union(_) => panic!("Cannot derive HasSchema for union"),
    };

    // if nargs != args.len() {
    //     panic!("Deriving HasSchema{} with {} arguments", nargs, args.len());
    //}

    quote! {
        impl #impl_generics dbschema::#trait_name for #ident #ty_generics #where_clause {
            fn #method_name() -> dbschema::DbSchema {
                #schema.into()
            }
        }
    }
}

fn fields_schema(
    fields: &Fields,
    serde_rename_all: &Option<SerdeRenameAll>,
    removed: &BTreeSet<String>,
) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(FieldsNamed { named, .. }) => {
            let fields = named
                .into_iter()
                .map(
                    |Field {
                         ident, ty, attrs, ..
                     }| {
                        let field_attrs = FieldAttrs::from(attrs.as_ref());
                        let id = rename(
                            ident.as_ref().unwrap(),
                            field_attrs.serde_rename.as_ref(),
                            serde_rename_all.as_ref(),
                        );
                        if field_attrs.argument.is_some() {
                            panic!(
                                "dbschema::argument is currently not supported \
								 (it is intended for currently unsupported \
								 deriving of HasSchema1, HasSchema2, ...)"
                            );
                        }
						if field_attrs.json && (field_attrs.flatten || field_attrs.serde_flatten) {
							panic!("fields cannot be both saved as json and flattened")
						}
						if field_attrs.flatten && field_attrs.serde_flatten {
							panic!("serde(flatten) implies dbschema(flatten)")
						}
						let schema = match field_attrs.json {
                            true => quote!(dbschema::JsonSchema::new()),
                            false => {
                                quote!(<#ty as dbschema::HasSchema>::schema())
                            }
                        };
                        let flatten = field_attrs.flatten.then(|| quote!(.flatten(#id)));
                        let serde_flatten = field_attrs.serde_flatten.then(|| quote!(.serde_flatten(#id).unwrap()));
                        let elastic_rename =
							field_attrs.elastic_rename.as_ref()
							.map(|es_name| quote!(.elastic_rename(#id, #es_name)));
                        let serde_default = field_attrs
                            .serde_default
                            .as_ref()
                            .map(|def| match def {
                                SerdeDefault::Default => quote!(.set_default(<#ty as std::default::Default>::default()).unwrap()),
								SerdeDefault::Function(f) => {
									let f = Ident::new(f,Span::call_site());
									quote!(.set_default(#f()).unwrap())
								}
                            });
                        quote! {
                            .field(#id, #schema #serde_default)
                                #flatten #serde_flatten #elastic_rename
                        }
                    },
                )
                .collect::<Vec<_>>();
            let removed = removed
                .iter()
                .map(|name| quote! {.removed(#name)})
                .collect::<Vec<_>>();
            quote! {
                dbschema::StructSchema::new()
                    #(#fields)*
                #(#removed)*
            }
        }
        Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => match unnamed.len() {
            1 => {
                let Field { ty, .. } = unnamed.first().unwrap();
                quote! { <#ty as dbschema::HasSchema>::schema() }
            }
            _ => {
                panic!("Tuple structs are not supported for HasSchema")
            }
        },
        Fields::Unit => quote! { dbschema::UnitSchema::new() },
    }
}
