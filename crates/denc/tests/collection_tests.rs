use denc::Denc;
use bytes::BytesMut;

#[test]
fn test_string_encode_decode() {
    let mut buf = BytesMut::new();
    let value = "Hello, Ceph!".to_string();
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = String::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_empty_string() {
    let mut buf = BytesMut::new();
    let value = String::new();
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = String::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_unicode_string() {
    let mut buf = BytesMut::new();
    let value = "Hello 世界 🌍".to_string();
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = String::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_vec_u32() {
    let mut buf = BytesMut::new();
    let value = vec![1u32, 2, 3, 4, 5];
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<u32>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_empty_vec() {
    let mut buf = BytesMut::new();
    let value: Vec<u32> = vec![];
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<u32>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_vec_string() {
    let mut buf = BytesMut::new();
    let value = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<String>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_nested_vec() {
    let mut buf = BytesMut::new();
    let value = vec![
        vec![1u32, 2, 3],
        vec![4, 5],
        vec![6, 7, 8, 9],
    ];
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<Vec<u32>>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_option_some() {
    let mut buf = BytesMut::new();
    let value = Some(42u32);
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Option::<u32>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_option_none() {
    let mut buf = BytesMut::new();
    let value: Option<u32> = None;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Option::<u32>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_option_string() {
    let mut buf = BytesMut::new();
    let value = Some("test".to_string());
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Option::<String>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_vec_option() {
    let mut buf = BytesMut::new();
    let value = vec![Some(1u32), None, Some(3)];
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<Option<u32>>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_option_vec() {
    let mut buf = BytesMut::new();
    let value = Some(vec![1u32, 2, 3]);
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Option::<Vec<u32>>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_string_encoded_size() {
    let s = "hello".to_string();
    assert_eq!(s.encoded_size(), Some(4 + 5)); // 4 bytes length + 5 bytes data
}

#[test]
fn test_vec_encoded_size() {
    let v = vec![1u32, 2, 3];
    assert_eq!(v.encoded_size(), Some(4 + 3 * 4)); // 4 bytes length + 3 * 4 bytes elements
}

#[test]
fn test_option_encoded_size() {
    let some_val = Some(42u32);
    assert_eq!(some_val.encoded_size(), Some(1 + 4)); // 1 byte tag + 4 bytes value
    
    let none_val: Option<u32> = None;
    assert_eq!(none_val.encoded_size(), Some(1)); // 1 byte tag only
}

#[test]
fn test_large_vec() {
    let mut buf = BytesMut::new();
    let value: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<u8>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_deeply_nested() {
    let mut buf = BytesMut::new();
    let value = vec![Some(vec![1u32, 2]), None, Some(vec![3, 4, 5])];
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = Vec::<Option<Vec<u32>>>::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}

#[test]
fn test_string_buffer_too_short() {
    let mut buf = BytesMut::new();
    let value = "hello".to_string();
    value.encode(&mut buf).unwrap();
    
    // Truncate the buffer
    let buf = buf.freeze().slice(0..7); // Only 7 bytes instead of 9 (4 + 5)
    let mut buf_ref = buf;
    
    let result = String::decode(&mut buf_ref);
    assert!(result.is_err());
}

#[test]
fn test_vec_partial_data() {
    let mut buf = BytesMut::new();
    let value = vec![1u32, 2, 3];
    value.encode(&mut buf).unwrap();
    
    // Truncate to only have 2 elements
    let buf = buf.freeze().slice(0..12); // 4 (length) + 2 * 4 (two elements)
    let mut buf_ref = buf;
    
    let result = Vec::<u32>::decode(&mut buf_ref);
    assert!(result.is_err());
}

#[test]
fn test_option_invalid_tag() {
    let mut buf = BytesMut::new();
    2u8.encode(&mut buf).unwrap(); // Invalid tag (should be 0 or 1)
    
    let mut buf = buf.freeze();
    let result = Option::<u32>::decode(&mut buf);
    assert!(result.is_err());
}
