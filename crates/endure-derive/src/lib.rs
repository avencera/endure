//! Derive macro for [`endure`](https://docs.rs/endure)

use std::collections::HashSet;

use heck::ToSnakeCase;
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, LitStr, Type, parse_macro_input};

/// Makes a struct of durable collections openable as persistent state
///
/// Every field must be a [`DurableMap`], [`DurableCell`], or [`DurableVec`], or be marked
/// `#[durable(skip)]`. The derive implements `Clone` and adds `open()`, `builder()`, and
/// `memory()` constructors, so do not also derive `Clone`
///
/// Struct attributes:
/// - `#[durable(name = "file_stem")]` sets the default file name; the default is the struct name
///   in snake case
///
/// Field attributes:
/// - `#[durable(skip)]` keeps the field in memory only; it starts as `Default::default()`
/// - `#[durable(table = "name")]` stores the field in a table with this name instead of the field
///   name, which lets a field be renamed without losing its data
/// - `#[durable(versions(V1, V2))]` lists earlier value types, oldest first. Stored values are
///   upgraded through `From` impls (`V1` into `V2`, `V2` into the current type) when the state
///   opens
///
/// [`DurableMap`]: https://docs.rs/endure/latest/endure/struct.DurableMap.html
/// [`DurableCell`]: https://docs.rs/endure/latest/endure/struct.DurableCell.html
/// [`DurableVec`]: https://docs.rs/endure/latest/endure/struct.DurableVec.html
#[proc_macro_derive(Durable, attributes(durable))]
pub fn derive_durable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

struct PersistedField {
    ident: Ident,
    ty: Type,
    table: String,
    versions: Vec<syn::Path>,
}

enum FieldKind {
    Persisted(Box<PersistedField>),
    Skipped(Ident),
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "durable state structs cannot be generic",
        ));
    }

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            name,
            "Durable can only be derived for structs",
        ));
    };

    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            name,
            "Durable needs a struct with named fields",
        ));
    };

    let file_name = struct_name(&input)?.unwrap_or_else(|| name.to_string().to_snake_case());

    let mut fields = Vec::new();
    let mut tables = HashSet::new();
    for field in &named.named {
        let ident = field.ident.clone().expect("named fields have identifiers");
        let kind = field_kind(ident, field)?;

        if let FieldKind::Persisted(persisted) = &kind {
            if persisted.table.starts_with("__endure") {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    "table names starting with `__endure` are reserved",
                ));
            }

            if !tables.insert(persisted.table.clone()) {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    format!("two fields are stored in table `{}`", persisted.table),
                ));
            }
        }

        fields.push(kind);
    }

    let all_idents: Vec<&Ident> = fields
        .iter()
        .map(|kind| match kind {
            FieldKind::Persisted(field) => &field.ident,
            FieldKind::Skipped(ident) => ident,
        })
        .collect();

    let specs = fields.iter().filter_map(|kind| match kind {
        FieldKind::Persisted(field) => {
            let field_name = field.ident.to_string();
            let table = &field.table;
            let version = current_version(field);
            Some(quote! {
                ::endure::__private::FieldSpec { name: #field_name, table: #table, version: #version }
            })
        }
        FieldKind::Skipped(_) => None,
    });

    let loads = fields.iter().map(|kind| match kind {
        FieldKind::Persisted(field) => {
            let ident = &field.ident;
            let ty = &field.ty;
            let table = &field.table;
            let version = current_version(field);
            let decode = decode_chain(field);
            quote! {
                #ident: loader.field::<#ty>(#table, #version, |__version: u32, __bytes: &[u8]| { #decode })?
            }
        }
        FieldKind::Skipped(ident) => quote! { #ident: ::core::default::Default::default() },
    });

    let takes = fields.iter().filter_map(|kind| match kind {
        FieldKind::Persisted(field) => {
            let ident = &field.ident;
            let field_name = ident.to_string();
            let table = &field.table;
            Some(quote! { changes.field(#field_name, #table, &mut self.#ident)?; })
        }
        FieldKind::Skipped(_) => None,
    });

    Ok(quote! {
        impl ::core::clone::Clone for #name {
            fn clone(&self) -> Self {
                Self { #(#all_idents: ::core::clone::Clone::clone(&self.#all_idents)),* }
            }
        }

        impl ::endure::__private::State for #name {
            const NAME: &'static str = #file_name;
            const APP: &'static str = ::core::env!("CARGO_PKG_NAME");

            fn fields() -> &'static [::endure::__private::FieldSpec] {
                const FIELDS: &[::endure::__private::FieldSpec] = &[#(#specs),*];
                FIELDS
            }

            fn load(loader: &mut ::endure::__private::Loader<'_>) -> ::endure::Result<Self> {
                ::core::result::Result::Ok(Self { #(#loads),* })
            }

            fn take_changes(&mut self, changes: &mut ::endure::__private::Changes) -> ::endure::Result<()> {
                #(#takes)*
                ::core::result::Result::Ok(())
            }
        }

        impl #name {
            /// Opens this state at its default location
            pub fn open() -> ::endure::Result<::endure::Durable<Self>> {
                ::endure::Durable::open()
            }

            /// Starts configuring where this state is stored
            pub fn builder() -> ::endure::Builder<Self> {
                ::endure::Durable::builder()
            }

            /// Opens a fresh copy of this state that lives only in memory
            pub fn memory() -> ::endure::Durable<Self> {
                ::endure::Durable::memory()
            }
        }
    })
}

fn struct_name(input: &DeriveInput) -> syn::Result<Option<String>> {
    let mut name = None;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("durable"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value: LitStr = meta.value()?.parse()?;
                name = Some(value.value());
                Ok(())
            } else {
                Err(meta.error("unknown durable struct attribute; expected `name`"))
            }
        })?;
    }

    Ok(name)
}

fn field_kind(ident: Ident, field: &syn::Field) -> syn::Result<FieldKind> {
    let mut skip = false;
    let mut table = None;
    let mut versions = Vec::new();

    for attr in field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("durable"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                skip = true;
                Ok(())
            } else if meta.path.is_ident("table") {
                let value: LitStr = meta.value()?.parse()?;
                table = Some(value.value());
                Ok(())
            } else if meta.path.is_ident("versions") {
                meta.parse_nested_meta(|version| {
                    versions.push(version.path);
                    Ok(())
                })
            } else {
                Err(meta.error(
                    "unknown durable field attribute; expected `skip`, `table`, or `versions`",
                ))
            }
        })?;
    }

    if skip {
        if table.is_some() || !versions.is_empty() {
            return Err(syn::Error::new_spanned(
                &field.ident,
                "a skipped field cannot also set `table` or `versions`",
            ));
        }

        return Ok(FieldKind::Skipped(ident));
    }

    let table = table.unwrap_or_else(|| ident.to_string());
    Ok(FieldKind::Persisted(Box::new(PersistedField {
        ident,
        ty: field.ty.clone(),
        table,
        versions,
    })))
}

fn current_version(field: &PersistedField) -> u32 {
    field.versions.len() as u32 + 1
}

// decodes bytes written at `__version`, then converts step by step up to the current item type
fn decode_chain(field: &PersistedField) -> TokenStream2 {
    let ty = &field.ty;
    let table = &field.table;
    let item = quote! { <#ty as ::endure::__private::Field>::Item };

    let arms = field.versions.iter().enumerate().map(|(index, old)| {
        let version = index as u32 + 1;
        let steps = field.versions[index + 1..].iter().map(|next| {
            quote! { let __value: #next = ::core::convert::From::from(__value); }
        });
        let value = Ident::new("__value", Span::call_site());

        quote! {
            #version => {
                let #value: #old = ::endure::__private::decode(#table, __bytes)?;
                #(#steps)*
                let #value: #item = ::core::convert::From::from(#value);
                ::core::result::Result::Ok(#value)
            }
        }
    });

    quote! {
        match __version {
            #(#arms)*
            _ => ::endure::__private::decode::<#item>(#table, __bytes),
        }
    }
}
