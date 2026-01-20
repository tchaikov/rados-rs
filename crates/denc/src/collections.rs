use crate::traits::Denc;
use bytes::{Buf, BufMut};
use std::io;

// Implement Denc for String (length-prefixed UTF-8)
impl Denc for String {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        // Encode length as u32
        let len = self.len() as u32;
        len.encode(buf)?;
        
        // Encode UTF-8 bytes
        buf.put_slice(self.as_bytes());
        Ok(())
    }
    
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        // Decode length
        let len = u32::decode(buf)? as usize;
        
        // Check buffer has enough data
        if buf.remaining() < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("buffer too short for string: expected {} bytes, got {}", len, buf.remaining())
            ));
        }
        
        // Read bytes
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        
        // Convert to String (validate UTF-8)
        String::from_utf8(bytes).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid UTF-8: {}", e))
        })
    }
    
    fn encoded_size(&self) -> Option<usize> {
        Some(4 + self.len())
    }
}

// Implement Denc for Vec<T>
impl<T: Denc> Denc for Vec<T> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        // Encode length as u32
        let len = self.len() as u32;
        len.encode(buf)?;
        
        // Encode each element
        for item in self {
            item.encode(buf)?;
        }
        
        Ok(())
    }
    
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        // Decode length
        let len = u32::decode(buf)? as usize;
        
        // Cap allocation to prevent DOS attacks
        let cap = len.min(1024 * 1024); // Max 1M elements initially
        let mut vec = Vec::with_capacity(cap);
        
        // Decode each element
        for _ in 0..len {
            vec.push(T::decode(buf)?);
        }
        
        Ok(vec)
    }
    
    fn encoded_size(&self) -> Option<usize> {
        // Only return size if all elements have known size
        let mut size = 4; // Length prefix
        for item in self {
            match item.encoded_size() {
                Some(s) => size += s,
                None => return None,
            }
        }
        Some(size)
    }
}

// Implement Denc for Option<T>
impl<T: Denc> Denc for Option<T> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        match self {
            Some(value) => {
                // Encode 1 for Some
                1u8.encode(buf)?;
                value.encode(buf)?;
            }
            None => {
                // Encode 0 for None
                0u8.encode(buf)?;
            }
        }
        Ok(())
    }
    
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        let tag = u8::decode(buf)?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::decode(buf)?)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid Option tag: {}", tag)
            )),
        }
    }
    
    fn encoded_size(&self) -> Option<usize> {
        match self {
            Some(value) => value.encoded_size().map(|s| 1 + s),
            None => Some(1),
        }
    }
}
