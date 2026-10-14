//! Derive macro for [`endure`](https://docs.rs/endure)

use std::collections::HashSet;

use heck::ToSnakeCase;
use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Data, DeriveInput, Fields, Ident, LitInt, LitStr, Token, Type, parenthesized, parse_macro_input,
};

/// Makes a struct of durable collections openable as persistent state
///
/// Every field must be a [`DurableMap`], [`DurableCell`], or [`DurableVec`], or be marked
/// `#[durable(skip)]`. The struct must also implement `Clone`, usually through
/// `#[derive(Clone, Durable)]`. The derive adds `open()`, `builder()`, and `memory()`
/// constructors
///
/// Struct attributes:
/// - `#[durable(name = "file_stem")]` sets the default file name; the default is the struct name
///   in snake case
///
/// Field attributes:
/// - `#[durable(skip)]` keeps the field in memory only; it starts as `Default::default()`
/// - `#[durable(table = "name")]` stores the field in a table with this name instead of the field
///   name, which lets a field be renamed without losing its data
/// - `#[durable(versions(V1 = 1, V2 = 2))]` lists earlier value types with their schema version
///   numbers, oldest first and consecutive. The current type gets the next number. Stored values
///   are upgraded through `From` impls (`V1` into `V2`, `V2` into the current type) when the state
///   opens. Drop the oldest entries once no stored data needs them; data older than the first
///   listed version then fails to open instead of being misread
/// - `#[durable(version = 3)]` sets the current version explicitly. Use it to keep the number
///   after dropping every old entry, or alongside `versions(..)` as a check
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
    // the field name without any `r#` prefix; also the default table name
    name: String,
    ty: Type,
    table: String,
    versions: Vec<OldVersion>,
    // set with `version = N`; otherwise one past the last listed version
    current: Option<u32>,
}

impl PersistedField {
    fn current_version(&self) -> u32 {
        self.current
            .or_else(|| self.versions.last().map(|old| old.number + 1))
            .unwrap_or(1)
    }

    fn oldest_version(&self) -> u32 {
        self.versions
            .first()
            .map_or_else(|| self.current_version(), |old| old.number)
    }
}

struct OldVersion {
    ty: Type,
    number: u32,
}

impl Parse for OldVersion {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ty = input.parse()?;
        input.parse::<Token![=]>()?;
        let number: LitInt = input.parse()?;
        Ok(Self {
            ty,
            number: number.base10_parse()?,
        })
    }
}

enum FieldKind {
    Persisted(Box<PersistedField>),
    Skipped(Ident),
}

// the path to the endure crate as the user's manifest names it, so a renamed dependency works
fn endure_path() -> TokenStream2 {
    match crate_name("endure") {
        Ok(FoundCrate::Name(name)) => {
            let name = Ident::new(&name, Span::call_site());
            quote! { ::#name }
        }
        // inside endure's own tests and examples, and when the lookup fails, use the real name
        Ok(FoundCrate::Itself) | Err(_) => quote! { ::endure },
    }
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let krate = endure_path();

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

    let file_name =
        struct_name(&input)?.unwrap_or_else(|| name.unraw().to_string().to_snake_case());

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

    let specs = fields.iter().filter_map(|kind| match kind {
        FieldKind::Persisted(field) => {
            let field_name = &field.name;
            let table = &field.table;
            let version = field.current_version();
            Some(quote! {
                #krate::__private::FieldSpec { name: #field_name, table: #table, version: #version }
            })
        }
        FieldKind::Skipped(_) => None,
    });

    let loads = fields.iter().map(|kind| match kind {
        FieldKind::Persisted(field) => {
            let ident = &field.ident;
            let ty = &field.ty;
            let table = &field.table;
            let oldest = field.oldest_version();
            let current = field.current_version();
            let decode = decode_chain(field, &krate);
            quote! {
                #ident: loader.field::<#ty>(
                    #table,
                    #krate::__private::Versions { oldest: #oldest, current: #current },
                    |__version: u32, __bytes: &[u8]| { #decode },
                )?
            }
        }
        // memory-only fields survive a reload and start empty on open
        FieldKind::Skipped(ident) => quote! {
            #ident: match previous {
                ::core::option::Option::Some(previous) => ::core::clone::Clone::clone(&previous.#ident),
                ::core::option::Option::None => ::core::default::Default::default(),
            }
        },
    });

    let takes = fields.iter().filter_map(|kind| match kind {
        FieldKind::Persisted(field) => {
            let ident = &field.ident;
            let field_name = &field.name;
            let table = &field.table;
            Some(
                quote! { changes.field(#field_name, #table, &mut self.#ident, &previous.#ident)?; },
            )
        }
        FieldKind::Skipped(_) => None,
    });

    Ok(quote! {
        impl #krate::__private::State for #name {
            const NAME: &'static str = #file_name;
            const APP: &'static str = ::core::env!("CARGO_PKG_NAME");

            fn fields() -> &'static [#krate::__private::FieldSpec] {
                const FIELDS: &[#krate::__private::FieldSpec] = &[#(#specs),*];
                FIELDS
            }

            #[allow(unused_variables)]
            fn load(
                loader: &mut #krate::__private::Loader<'_>,
                previous: ::core::option::Option<&Self>,
            ) -> #krate::Result<Self> {
                ::core::result::Result::Ok(Self { #(#loads),* })
            }

            #[allow(unused_variables)]
            fn take_changes(
                &mut self,
                previous: &Self,
                changes: &mut #krate::__private::Changes,
            ) -> #krate::Result<()> {
                #(#takes)*
                ::core::result::Result::Ok(())
            }
        }

        impl #name {
            /// Opens this state at its default location
            pub fn open() -> #krate::Result<#krate::Durable<Self>> {
                #krate::Durable::open()
            }

            /// Starts configuring where this state is stored
            pub fn builder() -> #krate::Builder<Self> {
                #krate::Durable::builder()
            }

            /// Opens a fresh copy of this state that lives only in memory
            pub fn memory() -> #krate::Durable<Self> {
                #krate::Durable::memory()
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
    let mut current = None;

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
            } else if meta.path.is_ident("version") {
                let value: LitInt = meta.value()?.parse()?;
                current = Some(value.base10_parse::<u32>()?);
                Ok(())
            } else if meta.path.is_ident("versions") {
                let content;
                parenthesized!(content in meta.input);
                let parsed = Punctuated::<OldVersion, Token![,]>::parse_terminated(&content)
                    .map_err(|error| {
                        syn::Error::new(
                            error.span(),
                            "expected `versions(OldType = 1, NewerType = 2, ..)`",
                        )
                    })?;
                versions.extend(parsed);
                Ok(())
            } else {
                Err(meta.error(
                    "unknown durable field attribute; expected `skip`, `table`, `version`, or `versions`",
                ))
            }
        })?;
    }

    if skip {
        if table.is_some() || !versions.is_empty() || current.is_some() {
            return Err(syn::Error::new_spanned(
                &field.ident,
                "a skipped field cannot also set `table`, `version`, or `versions`",
            ));
        }

        return Ok(FieldKind::Skipped(ident));
    }

    if let Some(first) = versions.first()
        && first.number == 0
    {
        return Err(syn::Error::new_spanned(
            &first.ty,
            "schema versions start at 1",
        ));
    }

    for pair in versions.windows(2) {
        if pair[1].number != pair[0].number + 1 {
            return Err(syn::Error::new_spanned(
                &pair[1].ty,
                format!(
                    "versions must be consecutive; expected {} after {}",
                    pair[0].number + 1,
                    pair[0].number
                ),
            ));
        }
    }

    if current == Some(0) {
        return Err(syn::Error::new_spanned(
            &field.ident,
            "schema versions start at 1",
        ));
    }

    if let (Some(current), Some(last)) = (current, versions.last())
        && current != last.number + 1
    {
        return Err(syn::Error::new_spanned(
            &last.ty,
            format!(
                "the last listed version must be {}, one before `version = {current}`",
                current - 1
            ),
        ));
    }

    let name = ident.unraw().to_string();
    let table = table.unwrap_or_else(|| name.clone());
    Ok(FieldKind::Persisted(Box::new(PersistedField {
        ident,
        name,
        ty: field.ty.clone(),
        table,
        versions,
        current,
    })))
}

// decodes bytes written at `__version`, then converts step by step up to the current item type
fn decode_chain(field: &PersistedField, krate: &TokenStream2) -> TokenStream2 {
    let ty = &field.ty;
    let table = &field.table;
    let item = quote! { <#ty as #krate::__private::Field>::Item };

    let arms = field.versions.iter().enumerate().map(|(index, old)| {
        let number = old.number;
        let old_ty = &old.ty;
        let steps = field.versions[index + 1..].iter().map(|next| {
            let next_ty = &next.ty;
            quote! { let __value: #next_ty = ::core::convert::From::from(__value); }
        });

        quote! {
            #number => {
                let __value: #old_ty = #krate::__private::decode(#table, __bytes)?;
                #(#steps)*
                let __value: #item = ::core::convert::From::from(__value);
                ::core::result::Result::Ok(__value)
            }
        }
    });

    quote! {
        match __version {
            #(#arms)*
            _ => #krate::__private::decode::<#item>(#table, __bytes),
        }
    }
}
