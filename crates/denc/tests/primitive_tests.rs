use denc::{Denc, FixedSize};
use bytes::{BytesMut, Bytes};

#[test]
fn test_u8_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 42u8;
    value.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1);
    
    let mut buf = buf.freeze();
    let decoded = u8::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_u16_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 0x1234u16;
    value.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 2);
    
    let mut buf = buf.freeze();
    let decoded = u16::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_u32_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 0x12345678u32;
    value.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 4);
    
    let mut buf = buf.freeze();
    let decoded = u32::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_u64_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 0x123456789ABCDEF0u64;
    value.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 8);
    
    let mut buf = buf.freeze();
    let decoded = u64::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_i8_encode_decode() {
    let mut buf = BytesMut::new();
    let value = -42i8;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = i8::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_i16_encode_decode() {
    let mut buf = BytesMut::new();
    let value = -1234i16;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = i16::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_i32_encode_decode() {
    let mut buf = BytesMut::new();
    let value = -123456789i32;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = i32::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_i64_encode_decode() {
    let mut buf = BytesMut::new();
    let value = -123456789012345i64;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = i64::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_bool_encode_decode() {
    let mut buf = BytesMut::new();
    true.encode(&mut buf).unwrap();
    false.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    assert_eq!(bool::decode(&mut buf).unwrap(), true);
    assert_eq!(bool::decode(&mut buf).unwrap(), false);
}

#[test]
fn test_f32_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 3.14159f32;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = f32::decode(&mut buf).unwrap();
    assert!((value - decoded).abs() < 1e-6);
}

#[test]
fn test_f64_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 3.14159265358979323846f64;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = f64::decode(&mut buf).unwrap();
    assert!((value - decoded).abs() < 1e-15);
}

#[test]
fn test_buffer_too_short() {
    let buf = Bytes::from_static(&[1, 2, 3]);
    let mut buf_ref = buf;
    
    // Try to decode u64 (needs 8 bytes) from only 3 bytes
    let result = u64::decode(&mut buf_ref);
    assert!(result.is_err());
}

#[test]
fn test_fixed_size_trait() {
    assert_eq!(u8::SIZE, 1);
    assert_eq!(u16::SIZE, 2);
    assert_eq!(u32::SIZE, 4);
    assert_eq!(u64::SIZE, 8);
    assert_eq!(i8::SIZE, 1);
    assert_eq!(i16::SIZE, 2);
    assert_eq!(i32::SIZE, 4);
    assert_eq!(i64::SIZE, 8);
    assert_eq!(bool::SIZE, 1);
    assert_eq!(f32::SIZE, 4);
    assert_eq!(f64::SIZE, 8);
}

#[test]
fn test_encoded_size() {
    assert_eq!(42u32.encoded_size(), Some(4));
    assert_eq!((-42i64).encoded_size(), Some(8));
    assert_eq!(true.encoded_size(), Some(1));
    assert_eq!(3.14f32.encoded_size(), Some(4));
}
