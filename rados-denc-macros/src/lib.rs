//! Procedural macros for deriving Ceph DENC implementations.
//!
//! This crate provides the derive macros re-exported by `rados-denc`, including
//! `Denc`, `StructVDenc`, `VersionedDenc`, and `ZeroCopyDencode`.
//! Most downstream crates should depend on `rados-denc` and use these macros through
//! its public re-exports instead of importing `rados-denc-macros` directly.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Parsed values from `#[denc(...)]` attributes.
struct DencAttrs {
    /// Crate path for generated code (default: `::rados_denc`).
    krate: TokenStream2,
    /// Encoding version for `VersionedDenc`.
    version: Option<u8>,
    /// Compat version for `VersionedDenc` (defaults to `version` if absent).
    compat: Option<u8>,
    /// Whether to emit `const FEATURE_DEPENDENT: bool = true` (bare flag).
    feature_dependent: bool,
    /// Fixed on-wire struct_v for `StructVDenc`.
    struct_v: Option<u8>,
    /// Minimum accepted decoded struct_v for `StructVDenc`.
    min_struct_v: Option<u8>,
    /// Enforce decoded struct_v == struct_v for `StructVDenc`.
    strict_struct_v: bool,
    /// Optional Ceph release label used in min-version errors.
    ceph_release: Option<syn::LitStr>,
}

impl DencAttrs {
    fn new() -> Self {
        Self {
            krate: quote! { ::rados_denc },
            version: None,
            compat: None,
            feature_dependent: false,
            struct_v: None,
            min_struct_v: None,
            strict_struct_v: false,
            ceph_release: None,
        }
    }

    fn parse_u8_expr(value: &syn::Expr, context: &str) -> Option<u8> {
        if let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(i),
            ..
        }) = value
        {
            Some(i.base10_parse::<u8>().expect(context))
        } else {
            None
        }
    }

    fn parse_litstr_expr(value: &syn::Expr) -> Option<syn::LitStr> {
        if let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(s),
            ..
        }) = value
        {
            Some(s.clone())
        } else {
            None
        }
    }

    /// Parse all items from `#[denc(...)]` attributes.
    ///
    /// Supports key-value pairs and bare flags:
    /// - `crate = "path"`   — override the rados-denc crate path
    /// - `version = N`      — encoding version (required for `VersionedDenc`)
    /// - `compat = N`       — compat version (defaults to `version`)
    /// - `struct_v = N`     — fixed struct version for `StructVDenc`
    /// - `min_struct_v = N` — minimum accepted decoded struct_v for `StructVDenc`
    /// - `strict_struct_v`  — enforce decoded struct_v == struct_v
    /// - `ceph_release = "..."` — release label for min-version checks
    /// - `feature_dependent` — emit `FEATURE_DEPENDENT = true` in generated impls
    fn parse(attrs: &[syn::Attribute]) -> Self {
        let mut out = Self::new();

        for attr in attrs {
            if !attr.path().is_ident("denc") {
                continue;
            }
            let Ok(metas) = attr.parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            ) else {
                continue;
            };
            for meta in metas {
                match meta {
                    syn::Meta::Path(path) if path.is_ident("feature_dependent") => {
                        out.feature_dependent = true;
                    }
                    syn::Meta::Path(path) if path.is_ident("strict_struct_v") => {
                        out.strict_struct_v = true;
                    }
                    syn::Meta::NameValue(nv) => {
                        let val = &nv.value;
                        if nv.path.is_ident("crate") {
                            if let Some(s) = Self::parse_litstr_expr(val) {
                                out.krate = s
                                    .parse()
                                    .expect("Invalid crate path in #[denc(crate = \"...\")]");
                            }
                        } else if nv.path.is_ident("version") {
                            out.version = Self::parse_u8_expr(val, "Invalid #[denc(version = N)]");
                        } else if nv.path.is_ident("compat") {
                            out.compat = Self::parse_u8_expr(val, "Invalid #[denc(compat = N)]");
                        } else if nv.path.is_ident("struct_v") {
                            out.struct_v =
                                Self::parse_u8_expr(val, "Invalid #[denc(struct_v = N)]");
                        } else if nv.path.is_ident("min_struct_v") {
                            out.min_struct_v =
                                Self::parse_u8_expr(val, "Invalid #[denc(min_struct_v = N)]");
                        } else if nv.path.is_ident("ceph_release") {
                            out.ceph_release = Self::parse_litstr_expr(val);
                        }
                    }
                    _ => {}
                }
            }
        }

        out
    }

    fn feature_dependent_const(&self) -> TokenStream2 {
        if self.feature_dependent {
            quote! { const FEATURE_DEPENDENT: bool = true; }
        } else {
            quote! {}
        }
    }
}

/// Shared codegen fragments for field-by-field encoding/decoding derives.
///
/// Generates encode, decode, size, and where-clause token streams for each
/// field, parameterized by the trait path (e.g., `Denc`).
#[derive(Default)]
struct FieldCodegen {
    encode_stmts: Vec<TokenStream2>,
    decode_fields: Vec<TokenStream2>,
    size_stmts: Vec<TokenStream2>,
    where_clauses: Vec<TokenStream2>,
}

impl FieldCodegen {
    fn from_fields<'a, I>(fields: I, krate: &TokenStream2, trait_name: &TokenStream2) -> Self
    where
        I: IntoIterator<Item = &'a syn::Field>,
    {
        let mut out = Self::default();

        for f in fields {
            let field_name = &f.ident;
            let field_type = &f.ty;

            out.encode_stmts.push(
                quote! { <#field_type as #krate::#trait_name>::encode(&self.#field_name, buf, features)?; },
            );
            out.decode_fields.push(
                quote! { #field_name: <#field_type as #krate::#trait_name>::decode(buf, features)? },
            );
            out.size_stmts.push(
                quote! { size += <#field_type as #krate::#trait_name>::encoded_size(&self.#field_name, features)?; },
            );
            out.where_clauses
                .push(quote! { #field_type: #krate::#trait_name });
        }

        out
    }
}

fn expect_named_fields<'a>(input: &'a DeriveInput, derive_name: &str) -> &'a syn::FieldsNamed {
    match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => fields,
            _ => panic!("{derive_name} derive only supports named fields"),
        },
        _ => panic!("{derive_name} can only be derived for structs"),
    }
}

/// Derive macro for field-by-field Denc encoding/decoding.
///
/// Generates a `Denc` implementation that encodes/decodes each named field
/// in declaration order, passing `features` through to each field's impl.
///
/// Requires every field type to implement `rados_denc::Denc`.
///
/// # Crate path
///
/// When deriving inside the `rados-denc` crate itself, add the helper attribute:
/// ```ignore
/// #[derive(crate::Denc)]
/// #[denc(crate = "crate")]
/// struct Foo { ... }
/// ```
/// External crates need no annotation; `::rados_denc` is the default.
///
/// # Example
///
/// ```ignore
/// #[derive(rados_denc::Denc)]
/// struct MyMessage {
///     version: u8,
///     payload: Bytes,
///     count: u32,
/// }
/// ```
#[proc_macro_derive(Denc, attributes(denc))]
pub fn derive_denc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = DencAttrs::parse(&input.attrs);
    let krate = &attrs.krate;
    let name = &input.ident;
    let fields = expect_named_fields(&input, "Denc");
    let denc = quote! { Denc };
    let codegen = FieldCodegen::from_fields(fields.named.iter(), krate, &denc);

    let encode_stmts = &codegen.encode_stmts;
    let decode_fields = &codegen.decode_fields;
    let size_stmts = &codegen.size_stmts;
    let where_clauses = &codegen.where_clauses;

    let expanded = quote! {
        impl #krate::Denc for #name
        where
            #(#where_clauses,)*
        {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                #(#encode_stmts)*
                ::std::result::Result::Ok(())
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<Self, #krate::RadosError>
            where
                Self: Sized,
            {
                ::std::result::Result::Ok(Self {
                    #(#decode_fields,)*
                })
            }

            fn encoded_size(&self, features: u64) -> ::std::option::Option<usize> {
                let mut size: usize = 0;
                #(#size_stmts)*
                ::std::option::Option::Some(size)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for zero-copy encoding/decoding of POD types.
///
/// See `Denc` derive for the `#[denc(crate = "...")]` attribute.
#[proc_macro_derive(ZeroCopyDencode, attributes(denc))]
pub fn derive_zerocopy_dencode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = DencAttrs::parse(&input.attrs);
    let krate = &attrs.krate;
    let name = &input.ident;

    assert!(
        matches!(&input.data, Data::Struct(_)),
        "ZeroCopyDencode can only be derived for structs"
    );
    let denc_impl = generate_zerocopy_denc(name, krate);

    let expanded = quote! {
        #denc_impl

        impl #krate::zero_copy::ZeroCopyDencode for #name {}
    };

    TokenStream::from(expanded)
}

fn generate_zerocopy_denc(name: &syn::Ident, krate: &TokenStream2) -> TokenStream2 {
    quote! {
        impl #krate::Denc for #name {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                let bytes = <Self as #krate::zerocopy::IntoBytes>::as_bytes(self);
                if buf.remaining_mut() < bytes.len() {
                    return ::std::result::Result::Err(#krate::RadosError::Protocol(format!(
                        "Insufficient buffer: need {}, have {}",
                        bytes.len(),
                        buf.remaining_mut()
                    )));
                }
                buf.put_slice(bytes);
                ::std::result::Result::Ok(())
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<Self, #krate::RadosError>
            where
                Self: Sized,
            {
                let size = ::std::mem::size_of::<Self>();
                if buf.remaining() < size {
                    return ::std::result::Result::Err(#krate::RadosError::Protocol(format!(
                        "Insufficient bytes: need {}, have {}",
                        size,
                        buf.remaining()
                    )));
                }
                let mut bytes = ::std::vec![0u8; size];
                buf.copy_to_slice(&mut bytes);
                <Self as #krate::zerocopy::FromBytes>::read_from_bytes(&bytes)
                    .map_err(|e| #krate::RadosError::Protocol(
                        format!("zerocopy decode failed: {:?}", e)
                    ))
            }

            fn encoded_size(&self, _features: u64) -> ::std::option::Option<usize> {
                ::std::option::Option::Some(::std::mem::size_of::<Self>())
            }
        }
    }
}

/// Derive macro for versioned Ceph encoding (ENCODE_START/DECODE_START pattern).
///
/// Generates both `impl VersionedEncode` and `impl Denc` for the struct.
/// Each field must implement `rados_denc::Denc`. Fields are encoded/decoded in
/// declaration order.
///
/// # Required attribute
///
/// ```ignore
/// #[denc(version = N)]           // encoding version
/// #[denc(version = N, compat = M)]  // encoding version + compat version (defaults to N)
/// ```
///
/// # Crate path
///
/// Same `#[denc(crate = "crate")]` convention as the `Denc` derive.
/// All options can be combined in one attribute:
/// ```ignore
/// #[denc(version = 1, compat = 1, crate = "crate")]
/// ```
///
/// # Example
///
/// ```ignore
/// #[derive(rados_denc::VersionedDenc)]
/// #[denc(version = 1, compat = 1)]
/// pub struct StoreStatfs {
///     pub total: u64,
///     pub available: u64,
///     // ...
/// }
/// ```
///
/// # Limitations
///
/// Only suitable for types with a **single fixed version**. Types with
/// version-range decoding or feature-dependent encoding must implement
/// `VersionedEncode` manually.
#[proc_macro_derive(VersionedDenc, attributes(denc))]
pub fn derive_versioned_denc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = DencAttrs::parse(&input.attrs);
    let krate = &attrs.krate;
    let name = &input.ident;

    let version = attrs
        .version
        .expect("#[denc(version = N)] is required when using #[derive(VersionedDenc)]");
    let compat = attrs.compat.unwrap_or(version);

    let (codegen, decode_expr) = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let denc = quote! { Denc };
                let cg = FieldCodegen::from_fields(fields.named.iter(), krate, &denc);
                let df = &cg.decode_fields;
                let expr = quote! { Self { #(#df,)* } };
                (cg, expr)
            }
            Fields::Unit => (FieldCodegen::default(), quote! { Self }),
            _ => panic!("VersionedDenc derive only supports named or unit structs"),
        },
        _ => panic!("VersionedDenc can only be derived for structs"),
    };

    let encode_stmts = &codegen.encode_stmts;
    let size_stmts = &codegen.size_stmts;
    let where_clauses = &codegen.where_clauses;
    let feature_dependent = attrs.feature_dependent_const();

    let expanded = quote! {
        impl #krate::VersionedEncode for #name
        where
            #(#where_clauses,)*
        {
            #feature_dependent
            const MAX_DECODE_VERSION: u8 = #version;

            fn encoding_version(&self, _features: u64) -> u8 {
                #version
            }

            fn compat_version(&self, _features: u64) -> u8 {
                #compat
            }

            fn encode_content<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
                _version: u8,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                #(#encode_stmts)*
                ::std::result::Result::Ok(())
            }

            fn decode_content<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
                _version: u8,
                compat_version: u8,
            ) -> ::std::result::Result<Self, #krate::RadosError> {
                if compat_version > #version {
                    return ::std::result::Result::Err(#krate::RadosError::Protocol(
                        ::std::format!(
                            concat!(stringify!(#name), " compat version {} is not supported (max: {})"),
                            compat_version,
                            #version,
                        ),
                    ));
                }
                ::std::result::Result::Ok(#decode_expr)
            }

            fn encoded_size_content(
                &self,
                features: u64,
                _version: u8,
            ) -> ::std::option::Option<usize> {
                let mut size: usize = 0;
                #(#size_stmts)*
                ::std::option::Option::Some(size)
            }
        }

        impl #krate::Denc for #name
        where
            #(#where_clauses,)*
        {
            const USES_VERSIONING: bool = true;
            #feature_dependent

            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                <Self as #krate::VersionedEncode>::encode_versioned(self, buf, features)
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<Self, #krate::RadosError> {
                <Self as #krate::VersionedEncode>::decode_versioned(buf, features)
            }

            fn encoded_size(&self, features: u64) -> ::std::option::Option<usize> {
                <Self as #krate::VersionedEncode>::encoded_size_versioned(self, features)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for struct_v-first encoding (no ENCODE_START wrapper).
///
/// This matches the common Ceph pattern:
/// `encode(struct_v, bl); encode(field1, bl); ...`
///
/// Required:
/// - `#[denc(struct_v = N)]`
///
/// Optional:
/// - `#[denc(min_struct_v = M)]`
/// - `#[denc(ceph_release = "...")]` (used with `min_struct_v`)
/// - `#[denc(strict_struct_v)]` (mutually exclusive with `min_struct_v`)
///
/// The first struct field must be named `struct_v` and have type `u8`.
/// When pairing this derive with `serde::Serialize` for Ceph-style `dump_json`
/// output, annotate the `struct_v` field with `#[serde(skip)]` so the wire-only
/// version byte is not emitted in JSON.
#[proc_macro_derive(StructVDenc, attributes(denc))]
pub fn derive_struct_v_denc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = DencAttrs::parse(&input.attrs);
    let krate = &attrs.krate;
    let name = &input.ident;

    let struct_v_lit = attrs
        .struct_v
        .expect("#[denc(struct_v = N)] is required when using #[derive(StructVDenc)]");

    assert!(
        !(attrs.strict_struct_v && attrs.min_struct_v.is_some()),
        "#[denc(strict_struct_v)] and #[denc(min_struct_v = N)] are mutually exclusive"
    );

    let fields = expect_named_fields(&input, "StructVDenc");

    let first_field = fields
        .named
        .iter()
        .next()
        .expect("StructVDenc requires at least one field");

    assert!(
        first_field
            .ident
            .as_ref()
            .is_some_and(|id| id == "struct_v"),
        "StructVDenc requires the first field to be named `struct_v`"
    );

    assert!(
        matches!(&first_field.ty, syn::Type::Path(tp) if tp.qself.is_none() && tp.path.is_ident("u8")),
        "StructVDenc requires `struct_v` to have type u8"
    );

    let denc = quote! { Denc };
    let codegen = FieldCodegen::from_fields(fields.named.iter().skip(1), krate, &denc);
    let encode_stmts = &codegen.encode_stmts;
    let decode_fields = &codegen.decode_fields;
    let size_stmts = &codegen.size_stmts;
    let where_clauses = &codegen.where_clauses;

    let version_check = if attrs.strict_struct_v {
        quote! {
            if struct_v != #struct_v_lit {
                return ::std::result::Result::Err(#krate::RadosError::Protocol(
                    ::std::format!(
                        concat!(stringify!(#name), " struct_v {} not supported (expected {})"),
                        struct_v,
                        #struct_v_lit,
                    ),
                ));
            }
        }
    } else if let Some(min_v) = attrs.min_struct_v {
        if let Some(release) = attrs.ceph_release.as_ref() {
            quote! { #krate::check_min_version!(struct_v, #min_v, stringify!(#name), #release); }
        } else {
            quote! {
                if struct_v < #min_v {
                    return ::std::result::Result::Err(#krate::RadosError::Protocol(
                        ::std::format!(
                            concat!(stringify!(#name), " struct_v {} not supported (minimum {})"),
                            struct_v,
                            #min_v,
                        ),
                    ));
                }
            }
        }
    } else {
        quote! {}
    };

    let feature_dependent = attrs.feature_dependent_const();

    let expanded = quote! {
        impl #krate::Denc for #name
        where
            #(#where_clauses,)*
        {
            #feature_dependent

            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                if self.struct_v != #struct_v_lit {
                    return ::std::result::Result::Err(#krate::RadosError::Protocol(
                        ::std::format!(
                            concat!(stringify!(#name), " struct_v {} does not match encoder version {}"),
                            self.struct_v,
                            #struct_v_lit,
                        ),
                    ));
                }
                <u8 as #krate::Denc>::encode(&self.struct_v, buf, 0)?;
                #(#encode_stmts)*
                ::std::result::Result::Ok(())
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<Self, #krate::RadosError>
            where
                Self: Sized,
            {
                let struct_v = <u8 as #krate::Denc>::decode(buf, features)?;
                #version_check
                ::std::result::Result::Ok(Self {
                    struct_v,
                    #(#decode_fields,)*
                })
            }

            fn encoded_size(&self, features: u64) -> ::std::option::Option<usize> {
                let mut size: usize = <u8 as #krate::Denc>::encoded_size(&#struct_v_lit, 0)?;
                #(#size_stmts)*
                ::std::option::Option::Some(size)
            }
        }
    };

    TokenStream::from(expanded)
}
