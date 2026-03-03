use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Parsed values from `#[denc(...)]` attributes.
struct DencAttrs {
    /// Crate path for generated code (default: `::denc`).
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
            krate: quote! { ::denc },
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
    /// - `crate = "path"`   — override the denc crate path
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
                    // Bare flag: `feature_dependent`
                    syn::Meta::Path(path) if path.is_ident("feature_dependent") => {
                        out.feature_dependent = true;
                    }
                    // Bare flag: `strict_struct_v`
                    syn::Meta::Path(path) if path.is_ident("strict_struct_v") => {
                        out.strict_struct_v = true;
                    }
                    // Key-value: `key = value`
                    syn::Meta::NameValue(nv) if nv.path.is_ident("crate") => {
                        if let Some(s) = Self::parse_litstr_expr(&nv.value) {
                            out.krate = s
                                .parse()
                                .expect("Invalid crate path in #[denc(crate = \"...\")]");
                        }
                    }
                    syn::Meta::NameValue(nv) if nv.path.is_ident("version") => {
                        out.version = Self::parse_u8_expr(
                            &nv.value,
                            "Invalid version in #[denc(version = N)]",
                        );
                    }
                    syn::Meta::NameValue(nv) if nv.path.is_ident("compat") => {
                        out.compat =
                            Self::parse_u8_expr(&nv.value, "Invalid compat in #[denc(compat = N)]");
                    }
                    syn::Meta::NameValue(nv) if nv.path.is_ident("struct_v") => {
                        out.struct_v = Self::parse_u8_expr(
                            &nv.value,
                            "Invalid struct_v in #[denc(struct_v = N)]",
                        );
                    }
                    syn::Meta::NameValue(nv) if nv.path.is_ident("min_struct_v") => {
                        out.min_struct_v = Self::parse_u8_expr(
                            &nv.value,
                            "Invalid min_struct_v in #[denc(min_struct_v = N)]",
                        );
                    }
                    syn::Meta::NameValue(nv) if nv.path.is_ident("ceph_release") => {
                        out.ceph_release = Self::parse_litstr_expr(&nv.value);
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

/// Shared codegen fragments for field-by-field Denc-style derives.
struct FieldCodegen {
    encode_stmts: Vec<TokenStream2>,
    decode_fields: Vec<TokenStream2>,
    size_stmts: Vec<TokenStream2>,
    where_clauses: Vec<TokenStream2>,
}

impl FieldCodegen {
    fn from_denc_fields<'a, I>(fields: I, krate: &TokenStream2) -> Self
    where
        I: IntoIterator<Item = &'a syn::Field>,
    {
        let fields: Vec<_> = fields.into_iter().collect();

        let encode_stmts = fields
            .iter()
            .map(|f| {
                let field_name = &f.ident;
                let field_type = &f.ty;
                quote! { <#field_type as #krate::Denc>::encode(&self.#field_name, buf, features)?; }
            })
            .collect();

        let decode_fields = fields
            .iter()
            .map(|f| {
                let field_name = &f.ident;
                let field_type = &f.ty;
                quote! {
                    #field_name: <#field_type as #krate::Denc>::decode(buf, features)?
                }
            })
            .collect();

        let size_stmts = fields
            .iter()
            .map(|f| {
                let field_name = &f.ident;
                let field_type = &f.ty;
                quote! { size += <#field_type as #krate::Denc>::encoded_size(&self.#field_name, features)?; }
            })
            .collect();

        let where_clauses = fields
            .iter()
            .map(|f| {
                let field_type = &f.ty;
                quote! { #field_type: #krate::Denc }
            })
            .collect();

        Self {
            encode_stmts,
            decode_fields,
            size_stmts,
            where_clauses,
        }
    }
}

fn is_fixed_size_primitive_or_array(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Array(_) => true,
        syn::Type::Path(tp) if tp.qself.is_none() && tp.path.segments.len() == 1 => {
            let ident = &tp.path.segments[0].ident;
            ident == "u8"
                || ident == "u16"
                || ident == "u32"
                || ident == "u64"
                || ident == "i32"
                || ident == "i64"
                || ident == "bool"
        }
        _ => false,
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

/// Return just the crate path from `#[denc(crate = "...")]` (for existing derives).
fn find_denc_crate(attrs: &[syn::Attribute]) -> TokenStream2 {
    DencAttrs::parse(attrs).krate
}

/// Derive macro for field-by-field Denc encoding/decoding.
///
/// Generates a `Denc` implementation that encodes/decodes each named field
/// in declaration order, passing `features` through to each field's impl.
///
/// Requires every field type to implement `denc::Denc`.
///
/// # Crate path
///
/// When deriving inside the `denc` crate itself, add the helper attribute:
/// ```ignore
/// #[derive(crate::Denc)]
/// #[denc(crate = "crate")]
/// struct Foo { ... }
/// ```
/// External crates need no annotation; `::denc` is the default.
///
/// # Example
///
/// ```ignore
/// #[derive(denc::Denc)]
/// struct MyMessage {
///     version: u8,
///     payload: Bytes,
///     count: u32,
/// }
/// ```
#[proc_macro_derive(Denc, attributes(denc))]
pub fn derive_denc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let krate = find_denc_crate(&input.attrs);
    let name = &input.ident;
    let fields = expect_named_fields(&input, "Denc");
    let codegen = FieldCodegen::from_denc_fields(fields.named.iter(), &krate);

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
    let krate = find_denc_crate(&input.attrs);
    let name = &input.ident;

    let denc_impl = match &input.data {
        Data::Struct(_) => generate_zerocopy_denc(name, &krate),
        _ => panic!("ZeroCopyDencode can only be derived for structs"),
    };

    let expanded = quote! {
        #denc_impl

        // Implement the marker trait
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

/// Derive macro for efficient buffer-based encoding/decoding.
///
/// See `Denc` derive for the `#[denc(crate = "...")]` attribute.
#[proc_macro_derive(DencMut, attributes(denc))]
pub fn derive_denc_mut(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let krate = find_denc_crate(&input.attrs);
    let name = &input.ident;
    let fields = expect_named_fields(&input, "DencMut");

    let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
    let field_types: Vec<_> = fields.named.iter().map(|f| &f.ty).collect();

    let encode_fields = field_names.iter().map(|name| {
        quote! { self.#name.encode(buf, features)?; }
    });

    let decode_fields = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;
        quote! {
            #field_name: <#field_type as #krate::DencMut>::decode(buf, features)?
        }
    });

    let size_calculations = field_names.iter().map(|name| {
        quote! { size += self.#name.encoded_size(features)?; }
    });

    let denc_mut_impl = quote! {
        impl #krate::DencMut for #name {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                if let Some(size) = self.encoded_size(features) {
                    if buf.remaining_mut() < size {
                        return ::std::result::Result::Err(
                            #krate::RadosError::Protocol(format!(
                                "Insufficient buffer space: need {} bytes, have {}",
                                size,
                                buf.remaining_mut()
                            ))
                        );
                    }
                }
                #(#encode_fields)*
                ::std::result::Result::Ok(())
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<Self, #krate::RadosError> {
                ::std::result::Result::Ok(Self {
                    #(#decode_fields,)*
                })
            }

            fn encoded_size(
                &self,
                features: u64,
            ) -> ::std::option::Option<usize> {
                let mut size = 0;
                #(#size_calculations)*
                ::std::option::Option::Some(size)
            }
        }
    };

    let all_fixed_size = fields
        .named
        .iter()
        .all(|f| is_fixed_size_primitive_or_array(&f.ty));

    let fixed_size_impl = if all_fixed_size {
        let size_sum = field_types.iter().map(|ty| {
            quote! { <#ty as #krate::FixedSize>::SIZE }
        });
        Some(quote! {
            impl #krate::FixedSize for #name {
                const SIZE: usize = #(#size_sum)+*;
            }
        })
    } else {
        None
    };

    let expanded = if let Some(fixed_size) = fixed_size_impl {
        quote! { #denc_mut_impl #fixed_size }
    } else {
        quote! { #denc_mut_impl }
    };

    TokenStream::from(expanded)
}

/// Derive macro for versioned Ceph encoding (ENCODE_START/DECODE_START pattern).
///
/// Generates both `impl VersionedEncode` and `impl Denc` for the struct.
/// Each field must implement `denc::Denc`. Fields are encoded/decoded in
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
/// #[derive(denc::VersionedDenc)]
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

    let (encode_stmts, decode_expr, size_stmts, where_clauses): (
        Vec<TokenStream2>,
        TokenStream2,
        Vec<TokenStream2>,
        Vec<TokenStream2>,
    ) = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let codegen = FieldCodegen::from_denc_fields(fields.named.iter(), krate);
                let decode_fields = codegen.decode_fields;

                let decode_expr = quote! {
                    Self {
                        #(#decode_fields,)*
                    }
                };

                (
                    codegen.encode_stmts,
                    decode_expr,
                    codegen.size_stmts,
                    codegen.where_clauses,
                )
            }
            Fields::Unit => (Vec::new(), quote! { Self }, Vec::new(), Vec::new()),
            _ => panic!("VersionedDenc derive only supports named or unit structs"),
        },
        _ => panic!("VersionedDenc can only be derived for structs"),
    };

    // Emit `const FEATURE_DEPENDENT: bool = true;` only when the flag is set.
    let feature_dependent_ve = attrs.feature_dependent_const();
    let feature_dependent_denc = feature_dependent_ve.clone();

    let expanded = quote! {
        impl #krate::VersionedEncode for #name
        where
            #(#where_clauses,)*
        {
            #feature_dependent_ve
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
            #feature_dependent_denc

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
#[proc_macro_derive(StructVDenc, attributes(denc))]
pub fn derive_struct_v_denc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = DencAttrs::parse(&input.attrs);
    let krate = &attrs.krate;
    let name = &input.ident;

    let struct_v_lit = attrs
        .struct_v
        .expect("#[denc(struct_v = N)] is required when using #[derive(StructVDenc)]");

    if attrs.strict_struct_v && attrs.min_struct_v.is_some() {
        panic!("#[denc(strict_struct_v)] and #[denc(min_struct_v = N)] are mutually exclusive");
    }

    let fields = expect_named_fields(&input, "StructVDenc");

    let first_field = fields
        .named
        .iter()
        .next()
        .expect("StructVDenc requires at least one field");

    let is_struct_v_name = first_field
        .ident
        .as_ref()
        .map(|id| id == "struct_v")
        .unwrap_or(false);
    if !is_struct_v_name {
        panic!("StructVDenc requires the first field to be named `struct_v`");
    }

    let is_u8 = match &first_field.ty {
        syn::Type::Path(tp) => tp.qself.is_none() && tp.path.is_ident("u8"),
        _ => false,
    };
    if !is_u8 {
        panic!("StructVDenc requires `struct_v` to have type u8");
    }

    let body_fields: Vec<_> = fields.named.iter().skip(1).collect();
    let codegen = FieldCodegen::from_denc_fields(body_fields.iter().copied(), krate);
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

    let feature_dependent_denc = attrs.feature_dependent_const();

    let expanded = quote! {
        impl #krate::Denc for #name
        where
            #(#where_clauses,)*
        {
            #feature_dependent_denc

            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> ::std::result::Result<(), #krate::RadosError> {
                if self.struct_v != (#struct_v_lit as u8) {
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
                let mut size: usize = <u8 as #krate::Denc>::encoded_size(&(#struct_v_lit as u8), 0)?;
                #(#size_stmts)*
                ::std::option::Option::Some(size)
            }
        }
    };

    TokenStream::from(expanded)
}
