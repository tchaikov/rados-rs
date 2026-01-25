# Unified Message Encoding/Decoding Framework

## Overview
Implemented a consolidated approach for encoding/decoding Ceph messages following the official Ceph "sandwich" structure.

## Architecture

### Message Structure (Wire Format)
Messages are serialized in the following order:
1. **Header** (ceph_msg_header) - 53 bytes
   - Contains metadata: seq, tid, type, version, lengths, src entity, CRC
2. **Old Footer** (ceph_msg_footer_old) - 13 bytes
   - Contains CRCs for front/middle/data sections and flags
   - Used for wire compatibility with older Ceph versions
3. **Payload (front)** - Variable length
   - Main message content (type-specific encoding)
4. **Middle** - Variable length (optional)
   - Optional middle section (rarely used)
5. **Data** - Variable length (optional)
   - Bulk data section (e.g., write data for MOSDOp)

### Key Components

#### 1. Core Framework (`crates/msgr2/src/ceph_message.rs`)

**CephMsgHeader**
- Represents the Ceph message header structure
- 53 bytes packed struct matching C++ definition
- Includes CRC calculation for header integrity

**CephMsgFooterOld**
- Old footer format for wire compatibility
- 13 bytes: 3 CRCs (u32 each) + flags (u8)
- Flags: FLAG_COMPLETE (1), FLAG_NOCRC (2)

**CephMessage**
- Complete message wrapper containing header, footer, and payload sections
- Methods:
  - `from_payload<T>()` - Create message from a payload type
  - `encode()` - Serialize to bytes (sandwich format)
  - `decode()` - Deserialize from bytes with CRC verification
  - `decode_payload<T>()` - Extract typed payload

**CephMessagePayload trait**
- Interface for message types to implement
- Methods:
  - `msg_type()` - Get message type constant
  - `msg_version()` - Get message version
  - `encode_payload()` - Encode front section
  - `encode_middle()` - Encode middle section (optional)
  - `encode_data()` - Encode data section (optional)
  - `decode_payload()` - Decode from sections

**CrcFlags**
- Bitflags for CRC calculation control
- DATA (1 << 0) - Calculate data CRCs
- HEADER (1 << 1) - Calculate header CRC
- ALL - Both flags

#### 2. OSD Message Implementation (`crates/osdclient/src/ceph_message_impl.rs`)

**MOSDOp Implementation**
- Message type: 42 (CEPH_MSG_OSD_OP)
- Version: 8
- Encodes payload using existing `MOSDOp::encode()`
- Encodes data section using `get_data_section()` for write operations

**MOSDOpReply Implementation**
- Message type: 43 (CEPH_MSG_OSD_OPREPLY)
- Version: 8
- Decodes payload using existing `MOSDOpReply::decode()`
- Handles data section distribution to operations

## Usage Example

```rust
use msgr2::ceph_message::{CephMessage, CephMessagePayload, CrcFlags};
use osdclient::messages::MOSDOp;

// Create a message
let mosdop = MOSDOp::new(/* ... */);
let msg = CephMessage::from_payload(&mosdop, 0, CrcFlags::ALL)?;

// Encode to bytes
let encoded = msg.encode()?;

// Decode from bytes
let decoded_msg = CephMessage::decode(&mut encoded.as_ref())?;

// Extract typed payload
let decoded_mosdop: MOSDOp = decoded_msg.decode_payload()?;
```

## Benefits

1. **Unified Approach**: Single framework for all message types
2. **Type Safety**: Trait-based design ensures correct implementation
3. **CRC Verification**: Automatic CRC calculation and verification
4. **Wire Compatibility**: Matches official Ceph wire format exactly
5. **Extensibility**: Easy to add new message types by implementing trait

## Testing

Tests added in `crates/osdclient/src/ceph_message_impl.rs`:
- `test_mosdop_message_encoding` - Basic MOSDOp encoding
- `test_mosdop_with_write_data` - MOSDOp with data section

All tests passing.

## References

- Ceph Message.h: ~/dev/ceph/src/msg/Message.h
- Ceph Message.cc: ~/dev/ceph/src/msg/Message.cc
- Linux msgr.h: ~/dev/linux/include/linux/ceph/msgr.h
- MOSDOpReply.h: ~/dev/ceph/src/messages/MOSDOpReply.h

## Future Work

- Add more message type implementations (MOSDMap, MAuth, etc.)
- Implement message compression support
- Add more comprehensive tests
- Consider adding message tracing support (Zipkin/Jaeger)
