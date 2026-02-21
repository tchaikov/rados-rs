use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Parsed values from `#[denc(key = value, ...)]` attributes.
struct DencAttrs {
    /// Crate path for generated code (default: `::denc`).
    krate: TokenStream2,
    /// Encoding version for `VersionedDenc`.
    version: Option<u8>,
    /// Compat version for `VersionedDenc` (defaults to `version` if absent).
    compat: Option<u8>,
}

/// Parse all key-value pairs from `#[denc(...)]` attributes.
///
/// Supports:
/// - `crate = "path"` — override the denc crate path
/// - `version = N`    — encoding version (required for `VersionedDenc`)
/// - `compat = N`     — compat version (defaults to `version`)
fn parse_denc_attrs(attrs: &[syn::Attribute]) -> DencAttrs {
    let mut krate = quote! { ::denc };
    let mut version = None;
    let mut compat = None;

    for attr in attrs {
        if !attr.path().is_ident("denc") {
            continue;
        }
        let Ok(nvs) = attr.parse_args_with(
            syn::punctuated::Punctuated::<syn::MetaNameValue, syn::Token![,]>::parse_terminated,
        ) else {
            continue;
        };
        for nv in nvs {
            if nv.path.is_ident("crate") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = nv.value
                {
                    krate = s
                        .parse()
                        .expect("Invalid crate path in #[denc(crate = \"...\")]");
                }
            } else if nv.path.is_ident("version") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(i),
                    ..
                }) = nv.value
                {
                    version = Some(
                        i.base10_parse::<u8>()
                            .expect("Invalid version in #[denc(version = N)]"),
                    );
                }
            } else if nv.path.is_ident("compat") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(i),
                    ..
                }) = nv.value
                {
                    compat = Some(
                        i.base10_parse::<u8>()
                            .expect("Invalid compat in #[denc(compat = N)]"),
                    );
                }
            }
        }
    }

    DencAttrs {
        krate,
        version,
        compat,
    }
}

/// Return just the crate path from `#[denc(crate = "...")]` (for existing derives).
fn find_denc_crate(attrs: &[syn::Attribute]) -> TokenStream2 {
    parse_denc_attrs(attrs).krate
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

    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => fields,
            _ => panic!("Denc derive only supports named fields"),
        },
        _ => panic!("Denc can only be derived for structs"),
    };

    let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
    let field_types: Vec<_> = fields.named.iter().map(|f| &f.ty).collect();

    let encode_stmts = field_names.iter().map(|name| {
        quote! { self.#name.encode(buf, features)?; }
    });

    let decode_fields = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;
        quote! {
            #field_name: <#field_type as #krate::Denc>::decode(buf, features)?
        }
    });

    let where_clauses = field_types.iter().map(|ty| {
        quote! { #ty: #krate::Denc }
    });

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
                #(size += self.#field_names.encoded_size(features)?;)*
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

    let (denc_mut_impl, fixed_size_impl) = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
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

                let all_fixed_size = fields.named.iter().all(|f| {
                    let type_str = quote!(#f.ty).to_string();
                    type_str == "u8"
                        || type_str == "u16"
                        || type_str == "u32"
                        || type_str == "u64"
                        || type_str == "i32"
                        || type_str == "i64"
                        || type_str == "bool"
                        || type_str.starts_with('[')
                });

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

                (denc_mut_impl, fixed_size_impl)
            }
            _ => panic!("DencMut only supports named fields"),
        },
        _ => panic!("DencMut can only be derived for structs"),
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
    let attrs = parse_denc_attrs(&input.attrs);
    let krate = &attrs.krate;
    let name = &input.ident;

    let version = attrs
        .version
        .expect("#[denc(version = N)] is required when using #[derive(VersionedDenc)]");
    let compat = attrs.compat.unwrap_or(version);

    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => fields,
            _ => panic!("VersionedDenc derive only supports named fields"),
        },
        _ => panic!("VersionedDenc can only be derived for structs"),
    };

    let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
    let field_types: Vec<_> = fields.named.iter().map(|f| &f.ty).collect();

    let encode_stmts = field_names.iter().map(|name| {
        quote! { self.#name.encode(buf, features)?; }
    });

    let decode_fields = fields.named.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;
        quote! {
            #field_name: <#field_type as #krate::Denc>::decode(buf, features)?
        }
    });

    let size_stmts = field_names.iter().map(|name| {
        quote! { size += self.#name.encoded_size(features)?; }
    });

    let where_clauses: Vec<_> = field_types
        .iter()
        .map(|ty| quote! { #ty: #krate::Denc })
        .collect();

    let expanded = quote! {
        impl #krate::VersionedEncode for #name
        where
            #(#where_clauses,)*
        {
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
                ::std::result::Result::Ok(Self {
                    #(#decode_fields,)*
                })
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
