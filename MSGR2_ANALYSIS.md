# msgr2 Protocol Implementation Analysis

## Current Status (2026-01-14)

### Test Results
Running `test_session_connecting` against local Ceph cluster:
- ✅ Banner exchange: SUCCESS
- ✅ HELLO exchange: SUCCESS
- ❌ AUTH_REQUEST: **FAILURE** - Server closes connection after receiving our AUTH_REQUEST

### Root Cause Analysis

#### Problem 1: Frame Architecture Confusion

The codebase has three different frame representations that are being mixed incorrectly:

1. **FrameTrait types** (`HelloFrame`, `AuthRequestFrame`, etc.)
   - Logical frame structures with typed fields
   - Have `to_wire()` method that returns complete wire-format `Bytes`
   - Example: `AuthRequestFrame::new(method, modes, payload).to_wire(&assembler, features)`

2. **Frame struct** (in state_machine.rs)
   ```rust
   pub struct Frame {
       pub preamble: Preamble,
       pub segments: Vec<Bytes>,  // Decoded segment data
   }
   ```
   - Simple container for state machine
   - Contains decoded preamble + raw segment bytes
   - Used for frame dispatch/routing

3. **Wire format** (`Bytes`)
   - Complete on-wire representation: preamble + segments + epilogue + CRCs
   - What actually goes on the TCP socket

**The Bug**: In `state_machine.rs` lines 317-333, we're doing:
```rust
let frame_bytes = auth_frame.to_wire(&mut assembler, 0)?;  // Returns COMPLETE wire format
let frame = Frame {
    preamble: ...,
    segments: vec![frame_bytes],  // ❌ WRONG: Wrapping wire format as segment
};
```

This creates a frame where the segment contains another complete frame (preamble+data+epilogue), which is invalid.

#### Problem 2: State Machine Frame Handling

The state machine should work with **decoded Frame structs**, not wire-format bytes:

**Correct flow should be**:
```
Send path:  FrameTrait → to_wire() → Bytes → TCP socket
Receive path: TCP socket → Bytes → from_wire() → Frame struct → State machine
```

**Current broken flow**:
```
Send: FrameTrait → to_wire() → Bytes → wrap in Frame → encode() → TCP
                                         ^^^^^^^^^^^^^^
                                         Double encoding!
```

### Comparison with C++ Implementation

From `/home/kefu/dev/ceph/src/msg/async/ProtocolV2.cc`:

```cpp
// C++ sends frames directly as wire format
auto frame = AuthRequestFrame::Encode(auth_method, preferred_modes, bl);
return WRITE(frame, "auth request", read_frame);
```

The C++ `WRITE` macro writes the frame directly to the socket. There's no intermediate "Frame" struct.

From `/home/kefu/dev/ceph/src/msg/async/ProtocolV2.h`:
- States are enum values, not separate structs
- Frame dispatch happens in `handle_read_frame_dispatch()` which switches on Tag
- No intermediate Frame struct - frames go directly from wire format to typed frames

### Architecture Recommendations

#### Option A: Remove Frame struct, use FrameTrait directly

**State machine works with FrameTrait types**:
```rust
pub enum StateResult {
    SendFrame(Box<dyn FrameTrait>),  // Send typed frame
    ...
}

impl State for AuthConnecting {
    fn enter(&mut self) -> Result<StateResult> {
        let auth_frame = AuthRequestFrame::new(2, vec![2, 1], payload);
        Ok(StateResult::SendFrame(Box::new(auth_frame)))
    }

    fn handle_frame(&mut self, tag: Tag, payload: Bytes) -> Result<StateResult> {
        match tag {
            Tag::AuthReplyMore => {
                let frame = AuthReplyMoreFrame::from_wire(payload)?;
                // Process frame...
            }
        }
    }
}
```

**Pros**:
- Matches C++ architecture
- Type-safe frame handling
- No intermediate Frame struct confusion

**Cons**:
- Requires trait objects or enum for StateResult
- More complex type signatures

#### Option B: Keep Frame struct, fix usage

**Frame struct contains decoded segments only**:
```rust
pub struct Frame {
    pub tag: Tag,
    pub segments: Vec<Bytes>,  // Raw decoded segments (no preamble/epilogue)
}

impl Frame {
    // Create from wire format
    pub fn from_wire(wire_bytes: Bytes) -> Result<Self> {
        // Parse preamble, read segments, verify CRCs
        // Return Frame with just the segment data
    }

    // Convert to wire format
    pub fn to_wire(&self, assembler: &mut FrameAssembler) -> Result<Bytes> {
        // Assemble preamble + segments + epilogue + CRCs
    }
}
```

**State machine uses Frame**:
```rust
impl State for AuthConnecting {
    fn enter(&mut self) -> Result<StateResult> {
        // Create typed frame
        let auth_frame = AuthRequestFrame::new(2, vec![2, 1], payload);

        // Convert to segments
        let segments = auth_frame.get_segments(0);

        // Create Frame struct
        let frame = Frame {
            tag: Tag::AuthRequest,
            segments,
        };

        Ok(StateResult::SendFrame(frame))
    }
}
```

**Pros**:
- Simpler state machine interface
- Clear separation of concerns
- Frame struct is just a transport container

**Cons**:
- Extra conversion step
- Frame struct is somewhat redundant

### Recommended Fix (Option B - Minimal Changes)

1. **Fix Frame struct usage in state_machine.rs**:
   - Don't call `to_wire()` inside state machine
   - Create Frame with decoded segments from FrameTrait

2. **Fix test code**:
   - `send_frame()` should call `to_wire()` on the Frame
   - `read_frame()` should parse wire format into Frame struct

3. **Update StateResult handling**:
   - When sending, convert Frame to wire format at the boundary
   - When receiving, parse wire format to Frame before state machine

### Files to Modify

1. **crates/msgr2/src/state_machine.rs** (lines 130-340)
   - Fix `HelloConnecting::enter()` - don't call `to_wire()`
   - Fix `AuthConnecting::enter()` - don't call `to_wire()`
   - Fix `SessionConnecting::enter()` - don't call `to_wire()`
   - All should create Frame with decoded segments

2. **crates/msgr2/examples/test_session_connecting.rs**
   - Fix `send_frame()` to call `to_wire()` on Frame
   - Verify `read_frame()` correctly parses wire format

3. **crates/msgr2/src/frames.rs**
   - Add helper methods to Frame struct for wire conversion
   - Ensure clear documentation of Frame vs wire format

### Next Steps

1. ✅ Document the architecture issue (this file)
2. ⏳ Implement Option B fixes
3. ⏳ Test with real Ceph cluster
4. ⏳ Verify AUTH_REQUEST is accepted
5. ⏳ Complete full handshake (AUTH_REPLY_MORE, AUTH_DONE, CLIENT_IDENT, SERVER_IDENT)

### Additional Observations

#### Auth Frame Types Status
All auth frame types are already defined in `frames.rs`:
- ✅ `AuthRequestFrame` (lines 635-641)
- ✅ `AuthBadMethodFrame` (lines 651-658)
- ✅ `AuthReplyMoreFrame` (lines 660-664)
- ✅ `AuthRequestMoreFrame` (lines 666-670)
- ✅ `AuthDoneFrame` (lines 643-649)

The frames are defined correctly using the `define_control_frame!` macro.

#### State Machine Auth Handling
The state machine in `state_machine.rs` has:
- ✅ `AuthConnecting` state (lines 167-340)
- ✅ Handles `Tag::AuthBadMethod` (line 223)
- ✅ Handles `Tag::AuthReplyMore` (line 226)
- ✅ Handles `Tag::AuthDone` (line 274)
- ✅ Multi-round auth logic (lines 226-273)

The logic is correct, just the frame encoding/decoding is broken.

#### C++ vs Rust Abstraction Layers

**C++ (ProtocolV2.cc)**:
```
State enum → handle_frame_dispatch() → handle_auth_reply_more() →
  AuthReplyMoreFrame::Decode() → Process → AuthRequestMoreFrame::Encode() →
  WRITE macro → TCP
```

**Rust (current)**:
```
State trait → handle_frame() → match tag →
  Process → Create FrameTrait → to_wire() → wrap in Frame → encode() → TCP
                                             ^^^^^^^^^^^^^^
                                             Extra layer causing bug
```

**Rust (should be)**:
```
State trait → handle_frame() → match tag →
  Process → Create FrameTrait → get_segments() → Frame →
  [at boundary] → to_wire() → TCP
```

### Testing Strategy

After fixes:
1. Run `test_session_connecting` - should complete full handshake
2. Verify AUTH_REQUEST is properly formatted
3. Verify AUTH_REPLY_MORE is handled correctly
4. Verify AUTH_DONE transitions to SESSION_CONNECTING
5. Verify CLIENT_IDENT/SERVER_IDENT exchange
6. Verify connection reaches READY state

### References

- msgr2 spec: `~/dev/ceph/doc/dev/msgr2.rst`
- C++ implementation: `~/dev/ceph/src/msg/async/ProtocolV2.{cc,h}`
- C++ frames: `~/dev/ceph/src/msg/async/frames_v2.{cc,h}`
- Plan document: `~/.claude/plans/swift-hopping-crown.md`
