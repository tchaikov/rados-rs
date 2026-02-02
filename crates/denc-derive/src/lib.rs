use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derive macro for zero-copy encoding/decoding of POD types
///
/// This generates implementations of the Denc trait. For repr(C, packed) structs,
/// it uses direct memory copy on little-endian systems and field-by-field encoding
/// on big-endian. Types are marked with the ZeroCopyDencode trait to indicate they
/// are safe for zero-copy operations.
///
/// All fields must implement the Denc trait.
#[proc_macro_derive(ZeroCopyDencode)]
pub fn derive_zerocopy_dencode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Check for repr(C, packed)
    let has_repr_c_packed = input.attrs.iter().any(|attr| {
        if attr.path().is_ident("repr") {
            // Try parsing as MetaList first
            if let Ok(list) = attr.parse_args::<syn::MetaList>() {
                let tokens = list.tokens.to_string();
                return tokens.contains("C") && tokens.contains("packed");
            }
            // Also try parsing the tokens directly
            let tokens = attr.meta.to_token_stream().to_string();
            return tokens.contains("C") && tokens.contains("packed");
        }
        false
    });

    let denc_impl = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();

                if has_repr_c_packed {
                    // Zero-copy path for repr(C, packed) structs - generate single Denc impl
                    generate_packed_denc(name, &field_names, &fields.named)
                } else {
                    // Standard field-by-field encoding - generate single Denc impl
                    generate_standard_denc(name, &field_names, &fields.named)
                }
            }
            _ => panic!("ZeroCopyDencode only supports named fields"),
        },
        _ => panic!("ZeroCopyDencode can only be derived for structs"),
    };

    let expanded = quote! {
        #denc_impl

        // Implement the marker trait
        impl denc::zerocopy::ZeroCopyDencode for #name {}
    };

    TokenStream::from(expanded)
}

fn generate_packed_denc(
    name: &syn::Ident,
    field_names: &[&Option<syn::Ident>],
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();

    let decode_fields = fields.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;

        quote! {
            #field_name: <#field_type as denc::Denc>::decode(buf, 0)?
        }
    });

    quote! {
        impl denc::Denc for #name
        where
            #(#field_types: denc::zerocopy::ZeroCopyDencode,)*
        {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<(), denc::RadosError> {
                let size = ::std::mem::size_of::<Self>();
                if buf.remaining_mut() < size {
                    return ::std::result::Result::Err(denc::RadosError::Protocol(format!(
                        "Insufficient buffer space: need {} bytes, have {}",
                        size,
                        buf.remaining_mut()
                    )));
                }

                // Zero-copy for little-endian systems
                #[cfg(target_endian = "little")]
                {
                    // SAFETY: Type is repr(C, packed) with primitive types,
                    // and we're on little-endian where memory layout matches wire format
                    unsafe {
                        let src_ptr = self as *const Self as *const u8;
                        let src_slice = ::std::slice::from_raw_parts(src_ptr, size);
                        buf.put_slice(src_slice);
                    }
                }

                // Field-by-field for big-endian
                // Use read_unaligned to safely read from packed struct fields
                #[cfg(not(target_endian = "little"))]
                {
                    unsafe {
                        #(
                            let field_val = ::std::ptr::addr_of!(self.#field_names).read_unaligned();
                            denc::Denc::encode(&field_val, buf, 0)?;
                        )*
                    }
                }

                ::std::result::Result::Ok(())
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<Self, denc::RadosError>
            where
                Self: Sized,
            {
                let size = ::std::mem::size_of::<Self>();
                if buf.remaining() < size {
                    return ::std::result::Result::Err(denc::RadosError::Protocol(format!(
                        "Insufficient bytes: need {} bytes, have {}",
                        size,
                        buf.remaining()
                    )));
                }

                // Zero-copy for little-endian systems
                #[cfg(target_endian = "little")]
                {
                    // SAFETY: Type is repr(C, packed) with primitive types,
                    // and we're on little-endian where memory layout matches wire format
                    unsafe {
                        let mut value = ::std::mem::MaybeUninit::<Self>::uninit();
                        let dst_ptr = value.as_mut_ptr() as *mut u8;
                        let dst_slice = ::std::slice::from_raw_parts_mut(dst_ptr, size);
                        buf.copy_to_slice(dst_slice);
                        ::std::result::Result::Ok(value.assume_init())
                    }
                }

                // Field-by-field for big-endian
                #[cfg(not(target_endian = "little"))]
                {
                    ::std::result::Result::Ok(Self {
                        #(#decode_fields,)*
                    })
                }
            }

            fn encoded_size(&self, _features: u64) -> ::std::option::Option<usize> {
                ::std::option::Option::Some(::std::mem::size_of::<Self>())
            }
        }
    }
}

fn generate_standard_denc(
    name: &syn::Ident,
    field_names: &[&Option<syn::Ident>],
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();

    let decode_fields = fields.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;

        quote! {
            #field_name: <#field_type as denc::Denc>::decode(buf, 0)?
        }
    });

    quote! {
        impl denc::Denc for #name
        where
            #(#field_types: denc::zerocopy::ZeroCopyDencode,)*
        {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<(), denc::RadosError> {
                #(
                    denc::Denc::encode(&self.#field_names, buf, 0)?;
                )*
                ::std::result::Result::Ok(())
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<Self, denc::RadosError>
            where
                Self: Sized,
            {
                ::std::result::Result::Ok(Self {
                    #(#decode_fields,)*
                })
            }

            fn encoded_size(&self, _features: u64) -> ::std::option::Option<usize> {
                let mut size = 0;
                #(
                    size += self.#field_names.encoded_size(0)?;
                )*
                ::std::option::Option::Some(size)
            }
        }
    }
}

/// Derive macro for efficient buffer-based encoding/decoding
///
/// This generates implementations of the `DencMut` trait, which enables zero-allocation
/// encoding by writing directly to mutable buffers. It also automatically implements
/// `FixedSize` when all fields have fixed size.
///
/// # Example
///
/// ```ignore
/// use denc::DencMut;
///
/// #[derive(DencMut)]
/// struct MyStruct {
///     field1: u32,
///     field2: u64,
///     field3: Vec<u8>,
/// }
/// ```
#[proc_macro_derive(DencMut)]
pub fn derive_denc_mut(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let (denc_mut_impl, fixed_size_impl) = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();
                let field_types: Vec<_> = fields.named.iter().map(|f| &f.ty).collect();

                // Generate encode implementation
                let encode_fields = field_names.iter().map(|name| {
                    quote! {
                        self.#name.encode(buf, features)?;
                    }
                });

                // Generate decode implementation
                let decode_fields = fields.named.iter().map(|f| {
                    let field_name = &f.ident;
                    let field_type = &f.ty;

                    quote! {
                        #field_name: <#field_type as denc::DencMut>::decode(buf, features)?
                    }
                });

                // Generate encoded_size implementation
                let size_calculations = field_names.iter().map(|name| {
                    quote! {
                        size += self.#name.encoded_size(features)?;
                    }
                });

                // Combined DencMut implementation
                let denc_mut_impl = quote! {
                    impl denc::DencMut for #name {
                        fn encode<B: bytes::BufMut>(&self, buf: &mut B, features: u64) -> ::std::result::Result<(), denc::RadosError> {
                            // Try to preallocate if we know the size
                            if let Some(size) = self.encoded_size(features) {
                                if buf.remaining_mut() < size {
                                    return ::std::result::Result::Err(denc::RadosError::Protocol(format!(
                                        "Insufficient buffer space: need {} bytes, have {}",
                                        size,
                                        buf.remaining_mut()
                                    )));
                                }
                            }

                            #(#encode_fields)*

                            ::std::result::Result::Ok(())
                        }

                        fn decode<B: bytes::Buf>(buf: &mut B, features: u64) -> ::std::result::Result<Self, denc::RadosError> {
                            ::std::result::Result::Ok(Self {
                                #(#decode_fields,)*
                            })
                        }

                        fn encoded_size(&self, features: u64) -> ::std::option::Option<usize> {
                            let mut size = 0;
                            #(#size_calculations)*
                            ::std::option::Option::Some(size)
                        }
                    }
                };

                // Check if all fields are FixedSize
                let all_fixed_size = fields.named.iter().all(|f| {
                    // This is a heuristic - we check if the type looks like a primitive
                    // In practice, the FixedSize trait bound will be checked at compile time
                    let type_str = quote!(#f.ty).to_string();
                    type_str == "u8"
                        || type_str == "u16"
                        || type_str == "u32"
                        || type_str == "u64"
                        || type_str == "i32"
                        || type_str == "i64"
                        || type_str == "bool"
                        || type_str.starts_with("[") // Arrays
                });

                let fixed_size_impl = if all_fixed_size {
                    let size_sum = field_types.iter().map(|ty| {
                        quote! {
                            <#ty as denc::FixedSize>::SIZE
                        }
                    });

                    Some(quote! {
                        impl denc::FixedSize for #name {
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

    // Combine all implementations
    let expanded = if let Some(fixed_size) = fixed_size_impl {
        quote! {
            #denc_mut_impl
            #fixed_size
        }
    } else {
        quote! {
            #denc_mut_impl
        }
    };

    TokenStream::from(expanded)
}
