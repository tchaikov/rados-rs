use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

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
            if let Ok(list) = attr.parse_args::<syn::MetaList>() {
                let tokens = list.tokens.to_string();
                return tokens.contains("C") && tokens.contains("packed");
            }
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
            if let Ok(list) = attr.parse_args::<syn::MetaList>() {
                let tokens = list.tokens.to_string();
                return tokens.contains("C") && tokens.contains("packed");
            }
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
