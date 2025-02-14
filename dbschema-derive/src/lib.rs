/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

mod attributes;
mod derive;
mod rename;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(HasSchema, attributes(dbschema))]
pub fn derive_has_schema(input: TokenStream) -> TokenStream {
    let tokens =
        derive::impl_has_schema(0, parse_macro_input!(input as DeriveInput));
    //eprintln!("HasSchema impl:\n{}", tokens);
    tokens.into()
}

// #[proc_macro_derive(HasSchema1, attributes(dbschema))]
// pub fn derive_has_schema1(input: TokenStream) -> TokenStream {
//     // let tokens =
//     //     derive::impl_has_schema(1, parse_macro_input!(input as DeriveInput));
//     // eprintln!("HasSchema1 impl:\n{}", tokens);
//     // tokens.into()
// }

// #[proc_macro_derive(HasSchema2, attributes(dbschema))]
// pub fn derive_has_schema2(input: TokenStream) -> TokenStream {
//     // let tokens =
//     //     derive::impl_has_schema(2, parse_macro_input!(input as DeriveInput));
//     // eprintln!("HasSchema2 impl:\n{}", tokens);
//     // tokens.into()
// }
