// Simple test for msgr v2.1 HELLO frame exchange
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing msgr v2.1 HELLO frame exchange...");
    
    // Connect to local Ceph cluster
    let mut stream = TcpStream::connect("127.0.0.1:6789").await?;
    println!("✓ Connected to Ceph monitor at 127.0.0.1:6789");
    
    // Phase 1: Banner exchange - send "ceph v2\n"
    let banner = b"ceph v2\n";
    stream.write_all(banner).await?;
    println!("✓ Sent banner: {:?}", std::str::from_utf8(banner)?);
    
    // Read banner response (expect 9 bytes: "ceph v2\n\0")
    let mut banner_response = vec![0u8; 9];
    stream.read_exact(&mut banner_response).await?;
    println!("✓ Received banner response: {:?}", String::from_utf8_lossy(&banner_response));
    
    // Phase 2: Send HELLO frame (msgr v2.1 protocol)
    // HELLO frame structure:
    // - Tag: 1 byte (0x01 for HELLO)
    // - Segment count: 1 byte
    // - Segment lengths: 4 bytes each
    // - Padding: to align to 8 bytes
    // - Payload data
    
    let mut hello_frame = BytesMut::new();
    
    // Preamble (32 bytes total)
    hello_frame.put_u8(0x01); // Tag::Hello
    hello_frame.put_u8(1);     // num_segments = 1
    
    // Build HELLO payload first to know its size
    let mut hello_payload = BytesMut::new();
    hello_payload.put_u8(1);  // entity_type (TYPE_CLIENT)
    
    // peer_addr (simplified EntityAddr)
    hello_payload.put_u32_le(1);  // type (TYPE_MSGR2)
    hello_payload.put_u32_le(0);  // nonce
    hello_payload.put_u32_le(0);  // sockaddr.ss_family (AF_UNSPEC)
    // padding for sockaddr
    for _ in 0..124 {
        hello_payload.put_u8(0);
    }
    
    // Segment length
    let payload_len = hello_payload.len() as u32;
    hello_frame.put_u32_le(payload_len);
    
    // Padding to make preamble 32 bytes
    let preamble_size = 2 + 4; // tag + num_segments + segment_length
    for _ in preamble_size..32 {
        hello_frame.put_u8(0);
    }
    
    // Add the payload
    hello_frame.extend_from_slice(&hello_payload);
    
    // Send HELLO frame
    stream.write_all(&hello_frame).await?;
    println!("✓ Sent HELLO frame ({} bytes)", hello_frame.len());
    
    // Read response (wait for any frame back)
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let mut response = vec![0u8; 1024];
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response)).await {
        Ok(Ok(n)) if n > 0 => {
            println!("✓ Received response: {} bytes", n);
            
            // Check if it's a valid frame response
            if n >= 32 {
                let tag = response[0];
                let num_segments = response[1];
                println!("  Response frame: tag={}, num_segments={}", tag, num_segments);
                
                // Tag values:
                // 2 = AuthRequest
                // 3 = AuthBadMethod
                // 8 = ClientIdent
                // 9 = ServerIdent
                match tag {
                    2 => println!("  → Server sent AUTH_REQUEST (expected for cephx auth)"),
                    3 => println!("  → Server sent AUTH_BAD_METHOD"),
                    8 => println!("  → Server sent CLIENT_IDENT"),
                    9 => println!("  → Server sent SERVER_IDENT"),
                    _ => println!("  → Server sent frame with tag {}", tag),
                }
            }
        }
        Ok(Ok(0)) => {
            println!("✗ Connection closed by server");
        }
        Ok(Err(e)) => {
            println!("✗ Read error: {}", e);
        }
        Err(_) => {
            println!("✗ Timeout waiting for response");
        }
    }
    
    println!("\n✓ HELLO frame test completed!");
    Ok(())
}