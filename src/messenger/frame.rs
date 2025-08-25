use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::{Error, Result};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameTag {
    Hello = 1,
    Auth = 2,
    AuthDone = 3,
    AuthBad = 4,
    AuthReply = 5,
    AuthMore = 6,
    AuthSignature = 7,
    ClientIdent = 8,
    ServerIdent = 9,
    IdentMissing = 10,
    Session = 11,
    ResetSession = 12,
    RetrySession = 13,
    RetryGlobal = 14,
    SessionReconnect = 15,
    SessionReset = 16,
    SessionRetry = 17,
    KeepAlive2 = 18,
    KeepAlive2Ack = 19,
    Ack = 20,
    MessageFrame = 21,
    Compression = 22,
    Hello2 = 23,
}

impl FrameTag {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::Auth),
            3 => Ok(Self::AuthDone),
            4 => Ok(Self::AuthBad),
            5 => Ok(Self::AuthReply),
            6 => Ok(Self::AuthMore),
            7 => Ok(Self::AuthSignature),
            8 => Ok(Self::ClientIdent),
            9 => Ok(Self::ServerIdent),
            10 => Ok(Self::IdentMissing),
            11 => Ok(Self::Session),
            12 => Ok(Self::ResetSession),
            13 => Ok(Self::RetrySession),
            14 => Ok(Self::RetryGlobal),
            15 => Ok(Self::SessionReconnect),
            16 => Ok(Self::SessionReset),
            17 => Ok(Self::SessionRetry),
            18 => Ok(Self::KeepAlive2),
            19 => Ok(Self::KeepAlive2Ack),
            20 => Ok(Self::Ack),
            21 => Ok(Self::MessageFrame),
            22 => Ok(Self::Compression),
            23 => Ok(Self::Hello2),
            _ => Err(Error::Protocol(format!("Invalid frame tag: {}", value))),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl fmt::Display for FrameTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Hello => "HELLO",
            Self::Auth => "AUTH",
            Self::AuthDone => "AUTH_DONE",
            Self::AuthBad => "AUTH_BAD",
            Self::AuthReply => "AUTH_REPLY",
            Self::AuthMore => "AUTH_MORE",
            Self::AuthSignature => "AUTH_SIGNATURE",
            Self::ClientIdent => "CLIENT_IDENT",
            Self::ServerIdent => "SERVER_IDENT",
            Self::IdentMissing => "IDENT_MISSING",
            Self::Session => "SESSION",
            Self::ResetSession => "RESET_SESSION",
            Self::RetrySession => "RETRY_SESSION",
            Self::RetryGlobal => "RETRY_GLOBAL",
            Self::SessionReconnect => "SESSION_RECONNECT",
            Self::SessionReset => "SESSION_RESET",
            Self::SessionRetry => "SESSION_RETRY",
            Self::KeepAlive2 => "KEEPALIVE2",
            Self::KeepAlive2Ack => "KEEPALIVE2_ACK",
            Self::Ack => "ACK",
            Self::MessageFrame => "MESSAGE",
            Self::Compression => "COMPRESSION",
            Self::Hello2 => "HELLO2",
        };
        write!(f, "{}", name)
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: FrameTag,
    pub payload_len: u32,
    pub payload: Bytes,
}

impl Frame {
    pub const HEADER_SIZE: usize = 5; // tag(1) + length(4)

    pub fn new(tag: FrameTag, payload: Bytes) -> Self {
        Self {
            tag,
            payload_len: payload.len() as u32,
            payload,
        }
    }

    pub fn hello() -> Self {
        Self::new(FrameTag::Hello, Bytes::new())
    }

    pub fn keepalive() -> Self {
        Self::new(FrameTag::KeepAlive2, Bytes::new())
    }

    pub fn keepalive_ack() -> Self {
        Self::new(FrameTag::KeepAlive2Ack, Bytes::new())
    }

    pub fn auth_request(payload: Bytes) -> Self {
        Self::new(FrameTag::Auth, payload)
    }

    pub fn auth_done() -> Self {
        Self::new(FrameTag::AuthDone, Bytes::new())
    }

    pub fn auth_bad() -> Self {
        Self::new(FrameTag::AuthBad, Bytes::new())
    }

    pub fn auth_reply(payload: Bytes) -> Self {
        Self::new(FrameTag::AuthReply, payload)
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<()> {
        if dst.remaining_mut() < Self::HEADER_SIZE + self.payload.len() {
            return Err(Error::BufferTooSmall { 
                needed: Self::HEADER_SIZE + self.payload.len() 
            });
        }

        dst.put_u8(self.tag.as_u8());
        dst.put_u32_le(self.payload_len);
        dst.extend_from_slice(&self.payload);

        Ok(())
    }

    pub fn decode_header(src: &mut impl Buf) -> Result<(FrameTag, u32)> {
        if src.remaining() < Self::HEADER_SIZE {
            return Err(Error::Deserialization("Incomplete frame header".into()));
        }

        let tag = FrameTag::from_u8(src.get_u8())?;
        let payload_len = src.get_u32_le();

        Ok((tag, payload_len))
    }

    pub fn decode_payload(src: &mut impl Buf, tag: FrameTag, payload_len: u32) -> Result<Self> {
        if src.remaining() < payload_len as usize {
            return Err(Error::Deserialization("Incomplete frame payload".into()));
        }

        let mut payload = vec![0u8; payload_len as usize];
        src.copy_to_slice(&mut payload);

        Ok(Self {
            tag,
            payload_len,
            payload: Bytes::from(payload),
        })
    }

    pub fn total_len(&self) -> usize {
        Self::HEADER_SIZE + self.payload.len()
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frame(tag={}, len={})", self.tag, self.payload_len)
    }
}