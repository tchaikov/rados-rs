# Macro-Based Simplification Opportunities

## Overview

After removing pre-Nautilus conditional decoding, several types now have straightforward field-by-field encoding/decoding with no conditionals. These are excellent candidates for using the `#[derive(Denc)]` macro instead of manual implementations.

## The `#[derive(Denc)]` Macro

The `denc-macros` crate provides a derive macro that automatically generates `Denc` implementations for structs with named fields. It:

1. Encodes each field in declaration order
2. Decodes each field in declaration order
3. Calculates encoded size by summing field sizes
4. Passes `features` through to each field's implementation

**Requirements**:
- All fields must implement `Denc`
- Fields must be named (not tuple structs)
- No conditional encoding/decoding logic needed
- No special transformations (like `pool as i64`)

## Candidates for Simplification

### ✅ Excellent Candidates (No Obstacles)

#### 1. **AuthTicket** (`crates/auth/src/types.rs:318`)

**Current**: Manual `Denc` implementation (40 lines)

**Structure**:
```rust
pub struct AuthTicket {
    struct_v: u8,
    pub name: EntityName,
    pub global_id: u64,
    old_auid: u64,
    pub created: SystemTime,
    pub expires: SystemTime,
    pub caps: AuthCapsInfo,
    pub flags: u32,
}
```

**Encoding**: All fields in order, no conditionals after v2 check

**Simplification**:
```rust
#[derive(Debug, Clone, denc::Denc)]
pub struct AuthTicket {
    struct_v: u8,
    pub name: EntityName,
    pub global_id: u64,
    old_auid: u64,
    pub created: SystemTime,
    pub expires: SystemTime,
    pub caps: AuthCapsInfo,
    pub flags: u32,
}

impl Denc for AuthTicket {
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let struct_v = u8::decode(buf, features)?;
        denc::check_min_version!(struct_v, 2, "AuthTicket", "Nautilus v14+");

        // Use derived decode for remaining fields
        let name = EntityName::decode(buf, features)?;
        let global_id = u64::decode(buf, features)?;
        let old_auid = u64::decode(buf, features)?;
        let created = SystemTime::decode(buf, features)?;
        let expires = SystemTime::decode(buf, features)?;
        let caps = AuthCapsInfo::decode(buf, features)?;
        let flags = u32::decode(buf, features)?;

        Ok(Self {
            struct_v,
            name,
            global_id,
            old_auid,
            created,
            expires,
            caps,
            flags,
        })
    }
}
```

**Issue**: The derive macro doesn't support custom decode logic for version checking. We need the version check at the start of decode.

**Alternative**: Keep manual implementation but it's already quite clean.

**Lines saved**: ~0 (version check prevents full macro usage)

#### 2. **CephXAuthenticate** (`crates/auth/src/protocol.rs:281`)

**Current**: Manual `Denc` implementation (40 lines)

**Structure**:
```rust
pub struct CephXAuthenticate {
    struct_v: u8,
    pub client_challenge: u64,
    pub key: u64,
    pub old_ticket: CephXTicketBlob,
    pub other_keys: u32,
}
```

**Same issue**: Needs version check at start of decode.

**Lines saved**: ~0 (version check prevents full macro usage)

### ❌ Not Suitable for Macro

#### 3. **HObject** (`crates/denc/src/hobject.rs:19`)

**Obstacles**:
1. Has `pool` field that needs `as i64` conversion for wire format
2. Has Hammer compatibility fix in decode (special case for pool == -1)
3. Uses `VersionedEncode` trait, not plain `Denc`

**Cannot use macro**: Type conversions and special logic required

#### 4. **MonInfo** (`crates/denc/src/monmap.rs:212`)

**Obstacles**:
1. Uses `VersionedEncode` trait, not plain `Denc`
2. Has feature-dependent encoding version
3. Has conditional encoding in `encode_content` (v2, v4, v5, v6 checks for backward compatibility)

**Cannot use macro**: Still has encoding conditionals for backward compatibility

#### 5. **MonMap** (`crates/denc/src/monmap.rs:321`)

**Obstacles**:
1. Uses `VersionedEncode` trait
2. Has feature-dependent encoding
3. Has conditional encoding for v7+, v8+, v9+ fields
4. Complex structure with many fields

**Cannot use macro**: Post-Nautilus conditionals still present

## Analysis: Why Macro Doesn't Help Much

### The Version Check Problem

The main obstacle is that types with version checks need custom decode logic:

```rust
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let struct_v = u8::decode(buf, features)?;

    // This check must happen BEFORE decoding other fields
    denc::check_min_version!(struct_v, 2, "AuthTicket", "Nautilus v14+");

    // Now decode remaining fields...
}
```

The `#[derive(Denc)]` macro generates code that decodes all fields in order, with no way to inject the version check between `struct_v` and the other fields.

### VersionedEncode vs Denc

Types using `VersionedEncode` have a different structure:
- `encode_content()` instead of `encode()`
- `decode_content()` instead of `decode()`
- Version header is added by the trait wrapper

The `#[derive(Denc)]` macro only works with the plain `Denc` trait.

### Encoding Conditionals Still Present

Even after simplification, some types still have encoding conditionals for forward compatibility:
- MonInfo: v2, v4, v5, v6 conditionals in encode
- MonMap: v7+, v8+, v9+ conditionals

These prevent using the macro for encoding.

## Potential Solutions

### Option 1: Enhanced Derive Macro with Version Checking

Add support for version checking to the derive macro:

```rust
#[derive(denc::Denc)]
#[denc(min_version = 2, version_field = "struct_v", release = "Nautilus v14+")]
pub struct AuthTicket {
    struct_v: u8,
    pub name: EntityName,
    // ... other fields
}
```

**Pros**:
- Could simplify types with version checks
- Consistent pattern across codebase

**Cons**:
- Significant macro complexity
- Only helps 2 types (AuthTicket, CephXAuthenticate)
- Marginal benefit (~20-30 lines saved total)

### Option 2: Split Struct Pattern

Split types into header + body:

```rust
#[derive(denc::Denc)]
pub struct AuthTicketBody {
    pub name: EntityName,
    pub global_id: u64,
    old_auid: u64,
    pub created: SystemTime,
    pub expires: SystemTime,
    pub caps: AuthCapsInfo,
    pub flags: u32,
}

pub struct AuthTicket {
    struct_v: u8,
    body: AuthTicketBody,
}

impl Denc for AuthTicket {
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let struct_v = u8::decode(buf, features)?;
        denc::check_min_version!(struct_v, 2, "AuthTicket", "Nautilus v14+");
        let body = AuthTicketBody::decode(buf, features)?;
        Ok(Self { struct_v, body })
    }

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.struct_v.encode(buf, features)?;
        self.body.encode(buf, features)?;
        Ok(())
    }
}
```

**Pros**:
- Uses macro for bulk of fields
- Clean separation of concerns

**Cons**:
- More complex type structure
- Breaks public API (fields now in `body`)
- Only saves ~10-15 lines per type
- Makes code less straightforward

### Option 3: Keep Manual Implementations

**Pros**:
- Current implementations are already clean and simple
- No version conditionals in decode after simplification
- Easy to understand and maintain
- No API changes needed

**Cons**:
- Slightly more code (~40 lines per type vs ~30 with macro)

## Recommendation

**Keep manual implementations** for the following reasons:

1. **Minimal Benefit**: Only 2 types (AuthTicket, CephXAuthenticate) could potentially benefit, saving ~20-30 lines total

2. **Version Check Requirement**: The need for version checking prevents straightforward macro usage

3. **Code Clarity**: Manual implementations are already very clean after removing conditionals:
   ```rust
   fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
       let struct_v = u8::decode(buf, features)?;
       denc::check_min_version!(struct_v, 2, "AuthTicket", "Nautilus v14+");

       let name = EntityName::decode(buf, features)?;
       let global_id = u64::decode(buf, features)?;
       // ... straightforward field decoding

       Ok(Self { struct_v, name, global_id, ... })
   }
   ```

4. **No Complexity**: Current code is easy to understand and maintain

5. **Consistency**: Types using `VersionedEncode` (HObject, MonInfo, MonMap) can't use the macro anyway

6. **Cost/Benefit**: Enhancing the macro or restructuring types would add complexity for minimal gain

## Types Already Using Derive Macro

These types successfully use `#[derive(Denc)]` because they have no version checks or special logic:

1. **CephXServerChallenge** (`crates/auth/src/protocol.rs:371`)
2. **CephXChallengeBlob** (`crates/auth/src/protocol.rs:394`)
3. **CephXServiceTicketRequest** (`crates/auth/src/protocol.rs:42`)
4. **Various message types** in monclient and osdclient

These are good examples of when the macro is appropriate.

## Conclusion

The pre-Nautilus simplification successfully removed ~135 lines of conditional logic, making the code much cleaner. However, further simplification using the `#[derive(Denc)]` macro is not practical because:

1. Version checks require custom decode logic
2. Only 2 types could potentially benefit
3. Savings would be minimal (~20-30 lines total)
4. Current implementations are already clean and maintainable

**Recommendation**: No further simplification needed. The current state after Phases 1-8 is optimal.
