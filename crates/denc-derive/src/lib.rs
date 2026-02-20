use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derive macro for zero-copy encoding/decoding of POD types
///
/// This generates a `Denc` implementation for the type using the `zerocopy`
/// crate's `IntoBytes`/`FromBytes` traits. All fields must implement
/// `ZeroCopyDencode`, which requires them to also implement zerocopy's traits.
///
/// Types must be `#[repr(C)]` and derive `FromBytes`, `IntoBytes`,
/// `KnownLayout`, and `Immutable` from the zerocopy crate.
#[proc_macro_derive(ZeroCopyDencode)]
pub fn derive_zerocopy_dencode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let denc_impl = match &input.data {
        Data::Struct(_) => generate_zerocopy_denc(name),
        _ => panic!("ZeroCopyDencode can only be derived for structs"),
    };

    let expanded = quote! {
        #denc_impl

        // Implement the marker trait
        impl denc::zero_copy::ZeroCopyDencode for #name {}
    };

    TokenStream::from(expanded)
}

fn generate_zerocopy_denc(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl denc::Denc for #name {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                _features: u64,
            ) -> ::std::result::Result<(), denc::RadosError> {
                let bytes = <Self as denc::zerocopy::IntoBytes>::as_bytes(self);
                if buf.remaining_mut() < bytes.len() {
                    return ::std::result::Result::Err(denc::RadosError::Protocol(format!(
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
            ) -> ::std::result::Result<Self, denc::RadosError>
            where
                Self: Sized,
            {
                let size = ::std::mem::size_of::<Self>();
                if buf.remaining() < size {
                    return ::std::result::Result::Err(denc::RadosError::Protocol(format!(
                        "Insufficient bytes: need {}, have {}",
                        size,
                        buf.remaining()
                    )));
                }
                let mut bytes = ::std::vec![0u8; size];
                buf.copy_to_slice(&mut bytes);
                <Self as denc::zerocopy::FromBytes>::read_from_bytes(&bytes)
                    .map_err(|e| denc::RadosError::Protocol(
                        format!("zerocopy decode failed: {:?}", e)
                    ))
            }

            fn encoded_size(&self, _features: u64) -> ::std::option::Option<usize> {
                ::std::option::Option::Some(::std::mem::size_of::<Self>())
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
