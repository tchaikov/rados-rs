use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derive macro for zero-copy encoding/decoding of POD types
///
/// This generates implementations of both Encode and Decode traits, as well as
/// the ZeroCopyDencode marker trait. For repr(C, packed) structs, it uses direct
/// memory copy on little-endian systems and field-by-field encoding on big-endian.
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

    let (encode_impl, decode_impl) = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();

                if has_repr_c_packed {
                    // Zero-copy path for repr(C, packed) structs
                    let encode = generate_packed_encode(name, &field_names);
                    let decode = generate_packed_decode(name, &fields.named);
                    (encode, decode)
                } else {
                    // Standard field-by-field encoding
                    let encode = generate_standard_encode(name, &field_names);
                    let decode = generate_standard_decode(name, &fields.named);
                    (encode, decode)
                }
            }
            _ => panic!("ZeroCopyDencode only supports named fields"),
        },
        _ => panic!("ZeroCopyDencode can only be derived for structs"),
    };

    let expanded = quote! {
        #encode_impl
        #decode_impl

        // Implement the marker trait
        impl denc::zerocopy::ZeroCopyDencode for #name {}
    };

    TokenStream::from(expanded)
}

fn generate_packed_encode(
    name: &syn::Ident,
    field_names: &[&Option<syn::Ident>],
) -> proc_macro2::TokenStream {
    quote! {
        impl denc::zerocopy::Encode for #name {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
            ) -> ::std::result::Result<(), denc::zerocopy::EncodeError> {
                let size = ::std::mem::size_of::<Self>();
                if buf.remaining_mut() < size {
                    return ::std::result::Result::Err(denc::zerocopy::EncodeError::InsufficientSpace {
                        required: size,
                        available: buf.remaining_mut(),
                    });
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
                            denc::zerocopy::Encode::encode(&field_val, buf)?;
                        )*
                    }
                }

                ::std::result::Result::Ok(())
            }

            fn encoded_size(&self) -> usize {
                ::std::mem::size_of::<Self>()
            }
        }
    }
}

fn generate_packed_decode(
    name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    let decode_fields = fields.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;

        quote! {
            #field_name: <#field_type as denc::zerocopy::Decode>::decode(buf)?
        }
    });

    quote! {
        impl denc::zerocopy::Decode for #name {
            fn decode<B: bytes::Buf>(
                buf: &mut B,
            ) -> ::std::result::Result<Self, denc::zerocopy::DecodeError> {
                let size = ::std::mem::size_of::<Self>();
                if buf.remaining() < size {
                    return ::std::result::Result::Err(denc::zerocopy::DecodeError::UnexpectedEof {
                        expected: size,
                        available: buf.remaining(),
                    });
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
        }
    }
}

fn generate_standard_encode(
    name: &syn::Ident,
    field_names: &[&Option<syn::Ident>],
) -> proc_macro2::TokenStream {
    quote! {
        impl denc::zerocopy::Encode for #name {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
            ) -> ::std::result::Result<(), denc::zerocopy::EncodeError> {
                #(
                    denc::zerocopy::Encode::encode(&self.#field_names, buf)?;
                )*
                ::std::result::Result::Ok(())
            }

            fn encoded_size(&self) -> usize {
                let mut size = 0;
                #(
                    size += self.#field_names.encoded_size();
                )*
                size
            }
        }
    }
}

fn generate_standard_decode(
    name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> proc_macro2::TokenStream {
    let decode_fields = fields.iter().map(|f| {
        let field_name = &f.ident;
        let field_type = &f.ty;

        quote! {
            #field_name: <#field_type as denc::zerocopy::Decode>::decode(buf)?
        }
    });

    quote! {
        impl denc::zerocopy::Decode for #name {
            fn decode<B: bytes::Buf>(
                buf: &mut B,
            ) -> ::std::result::Result<Self, denc::zerocopy::DecodeError> {
                ::std::result::Result::Ok(Self {
                    #(#decode_fields,)*
                })
            }
        }
    }
}

/// Derive macro for encoding structs to network byte order (little endian)
///
/// This generates an implementation that directly copies memory on little-endian systems
/// for repr(C, packed) structs with only primitive types.
#[proc_macro_derive(ZeroCopyEncode)]
pub fn derive_zerocopy_encode(input: TokenStream) -> TokenStream {
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

    let encode_impl = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();

                if has_repr_c_packed {
                    // Zero-copy path for repr(C, packed) structs on little-endian
                    quote! {
                        impl denc::zerocopy::Encode for #name {
                            fn encode<B: bytes::BufMut>(&self, buf: &mut B) -> ::std::result::Result<(), denc::zerocopy::EncodeError> {
                                let size = ::std::mem::size_of::<Self>();
                                if buf.remaining_mut() < size {
                                    return ::std::result::Result::Err(denc::zerocopy::EncodeError::InsufficientSpace {
                                        required: size,
                                        available: buf.remaining_mut(),
                                    });
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
                                            denc::zerocopy::Encode::encode(&field_val, buf)?;
                                        )*
                                    }
                                }

                                ::std::result::Result::Ok(())
                            }

                            fn encoded_size(&self) -> usize {
                                ::std::mem::size_of::<Self>()
                            }
                        }
                    }
                } else {
                    // Standard field-by-field encoding
                    quote! {
                        impl denc::zerocopy::Encode for #name {
                            fn encode<B: bytes::BufMut>(&self, buf: &mut B) -> ::std::result::Result<(), denc::zerocopy::EncodeError> {
                                #(
                                    denc::zerocopy::Encode::encode(&self.#field_names, buf)?;
                                )*
                                ::std::result::Result::Ok(())
                            }

                            fn encoded_size(&self) -> usize {
                                let mut size = 0;
                                #(
                                    size += self.#field_names.encoded_size();
                                )*
                                size
                            }
                        }
                    }
                }
            }
            _ => panic!("ZeroCopyEncode only supports named fields"),
        },
        _ => panic!("ZeroCopyEncode can only be derived for structs"),
    };

    TokenStream::from(encode_impl)
}

/// Derive macro for decoding structs from network byte order (little endian)
#[proc_macro_derive(ZeroCopyDecode)]
pub fn derive_zerocopy_decode(input: TokenStream) -> TokenStream {
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

    let decode_impl = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => {
                let decode_fields = fields.named.iter().map(|f| {
                    let field_name = &f.ident;
                    let field_type = &f.ty;

                    quote! {
                        #field_name: <#field_type as denc::zerocopy::Decode>::decode(buf)?
                    }
                });

                if has_repr_c_packed {
                    // Zero-copy path for repr(C, packed) structs
                    quote! {
                        impl denc::zerocopy::Decode for #name {
                            fn decode<B: bytes::Buf>(buf: &mut B) -> ::std::result::Result<Self, denc::zerocopy::DecodeError> {
                                let size = ::std::mem::size_of::<Self>();
                                if buf.remaining() < size {
                                    return ::std::result::Result::Err(denc::zerocopy::DecodeError::UnexpectedEof {
                                        expected: size,
                                        available: buf.remaining(),
                                    });
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
                        }
                    }
                } else {
                    // Standard field-by-field decoding
                    quote! {
                        impl denc::zerocopy::Decode for #name {
                            fn decode<B: bytes::Buf>(buf: &mut B) -> ::std::result::Result<Self, denc::zerocopy::DecodeError> {
                                ::std::result::Result::Ok(Self {
                                    #(#decode_fields,)*
                                })
                            }
                        }
                    }
                }
            }
            _ => panic!("ZeroCopyDecode only supports named fields"),
        },
        _ => panic!("ZeroCopyDecode can only be derived for structs"),
    };

    TokenStream::from(decode_impl)
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
