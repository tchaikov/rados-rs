//! Type-safe channels for routing Ceph map messages.
//!
//! Provides compile-time type safety for routing specific message types
//! (e.g., MOSDMap, MMDSMap) from connections to consumers.

use std::marker::PhantomData;
use tokio::sync::mpsc;

use crate::Message;

/// Trait for messages that can be routed through MapChannels.
///
/// Implementors must be Send + 'static and provide:
/// - MSG_TYPE: The Ceph message type constant (e.g., CEPH_MSG_OSD_MAP)
/// - NAME: Human-readable name for logging
pub trait MapMessage: Send + 'static {
    const MSG_TYPE: u16;
    const NAME: &'static str;
}

/// Sender half of a type-safe map channel.
///
/// Cloneable sender that can only send messages of type T.
/// Uses PhantomData<fn() -> T> for proper variance.
#[derive(Clone)]
pub struct MapSender<T: MapMessage> {
    tx: mpsc::Sender<Message>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: MapMessage> MapSender<T> {
    /// Send a message through the channel.
    ///
    /// Returns Ok(()) if sent, Err(message) if the receiver was dropped.
    pub async fn send(&self, msg: Message) -> Result<(), Message> {
        self.tx.send(msg).await.map_err(|e| e.0)
    }
}

/// Receiver half of a type-safe map channel.
///
/// Non-cloneable receiver that enforces single consumer at compile time.
pub struct MapReceiver<T: MapMessage> {
    rx: mpsc::Receiver<Message>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: MapMessage> MapReceiver<T> {
    /// Receive the next message from the channel.
    ///
    /// Returns None when all senders have been dropped.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

/// Create a bounded map channel for a specific message type.
///
/// # Arguments
/// * `buffer` - Maximum number of messages to buffer before backpressure
///
/// # Example
/// ```ignore
/// let (tx, rx) = map_channel::<MOSDMap>(64);
/// // tx can only send MOSDMap messages
/// // rx can only receive MOSDMap messages
/// // Compile error if you try to mix types
/// ```
pub fn map_channel<T: MapMessage>(buffer: usize) -> (MapSender<T>, MapReceiver<T>) {
    let (tx, rx) = mpsc::channel(buffer);
    (
        MapSender {
            tx,
            _phantom: PhantomData,
        },
        MapReceiver {
            rx,
            _phantom: PhantomData,
        },
    )
}
