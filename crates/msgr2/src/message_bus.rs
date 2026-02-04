//! Message bus for routing messages between components
//!
//! Inspired by PCIe bus architecture, the MessageBus provides a central routing
//! fabric for inter-component message delivery. Applications implement the
//! Dispatcher trait to receive messages of specific types.

use crate::message::Message;
use async_trait::async_trait;
use denc::RadosError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Dispatcher trait - implement this to receive messages
///
/// The Dispatcher trait is the core interface for handling messages in the
/// message bus architecture. Components (like OSDClient) implement this trait
/// to receive and process messages of specific types.
///
/// # Example
///
/// ```rust,ignore
/// #[async_trait]
/// impl Dispatcher for OSDClient {
///     async fn dispatch(&self, msg: Message) -> Result<(), RadosError> {
///         self.handle_osdmap(msg).await
///     }
/// }
/// ```
#[async_trait]
pub trait Dispatcher: Send + Sync {
    /// Dispatch a message to this handler
    ///
    /// This method is called directly from Connection's message loop when a
    /// message of the registered type is received. The handler should process
    /// the message and return Ok(()) on success or an error on failure.
    async fn dispatch(&self, msg: Message) -> Result<(), RadosError>;
}

/// Message bus routes inter-component messages
///
/// The MessageBus maintains a registry of message handlers (Dispatchers) indexed
/// by message type. When a message is dispatched through the bus, it is delivered
/// to all registered handlers for that message type in registration order.
///
/// # Architecture
///
/// - **Zero coupling**: Components communicate only through the bus
/// - **Direct dispatch**: Handlers are called in-line (no queuing)
/// - **Multi-subscriber**: Multiple handlers can register for the same message type
/// - **Error on unhandled**: Returns error if no handlers registered (catches bugs)
///
/// # Example
///
/// ```rust,ignore
/// // Create bus
/// let bus = Arc::new(MessageBus::new());
///
/// // Register handler
/// let osd_client = Arc::new(OSDClient::new(...));
/// bus.register(CEPH_MSG_OSD_MAP, osd_client).await;
///
/// // Dispatch message (called from Connection's message loop)
/// bus.dispatch(msg).await?;
/// ```
/// Type alias for the handler registry
type HandlerRegistry = Arc<RwLock<HashMap<u16, Vec<Arc<dyn Dispatcher>>>>>;

pub struct MessageBus {
    /// Map of message type -> list of registered dispatchers
    handlers: HandlerRegistry,
}

impl MessageBus {
    /// Create a new message bus
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a dispatcher for a message type
    ///
    /// The dispatcher will be called whenever a message of the specified type
    /// is dispatched through the bus. Multiple dispatchers can register for
    /// the same message type.
    ///
    /// # Arguments
    ///
    /// * `msg_type` - The message type to register for (e.g., CEPH_MSG_OSD_MAP)
    /// * `dispatcher` - The dispatcher to invoke for this message type
    pub async fn register(&self, msg_type: u16, dispatcher: Arc<dyn Dispatcher>) {
        let mut handlers = self.handlers.write().await;
        handlers.entry(msg_type).or_default().push(dispatcher);
    }

    /// Dispatch a message to all registered handlers
    ///
    /// This method is called from Connection's message loop when a message
    /// cannot be handled locally. It invokes all registered dispatchers for
    /// the message type in registration order.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to dispatch
    ///
    /// # Returns
    ///
    /// * `Ok(())` if at least one handler processed the message successfully
    /// * `Err(RadosError::UnhandledMessage)` if no handlers are registered
    /// * `Err(...)` if a handler returns an error
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No handlers are registered for the message type (bug)
    /// - A handler returns an error during dispatch
    pub async fn dispatch(&self, msg: Message) -> Result<(), RadosError> {
        let msg_type = msg.header.msg_type;
        let handlers = self.handlers.read().await;

        if let Some(handlers) = handlers.get(&msg_type) {
            if handlers.is_empty() {
                return Err(RadosError::Protocol(format!(
                    "No handlers registered for message type 0x{:04x}",
                    msg_type
                )));
            }

            // Dispatch to all handlers (in registration order)
            for handler in handlers {
                handler.dispatch(msg.clone()).await?;
            }
            Ok(())
        } else {
            Err(RadosError::Protocol(format!(
                "No handlers registered for message type 0x{:04x}",
                msg_type
            )))
        }
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU32, Ordering};

    // Test dispatcher that counts invocations
    struct TestDispatcher {
        msg_type: u16,
        count: Arc<AtomicU32>,
    }

    impl TestDispatcher {
        fn new(msg_type: u16) -> (Arc<Self>, Arc<AtomicU32>) {
            let count = Arc::new(AtomicU32::new(0));
            let dispatcher = Arc::new(Self {
                msg_type,
                count: count.clone(),
            });
            (dispatcher, count)
        }
    }

    #[async_trait]
    impl Dispatcher for TestDispatcher {
        async fn dispatch(&self, msg: Message) -> Result<(), RadosError> {
            // Copy msg_type to avoid packed struct reference
            let msg_type = msg.header.msg_type;
            assert_eq!(msg_type, self.msg_type);
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn create_test_message(msg_type: u16) -> Message {
        Message::new(msg_type, Bytes::new())
    }

    #[tokio::test]
    async fn test_single_dispatcher() {
        let bus = MessageBus::new();
        let (dispatcher, count) = TestDispatcher::new(0x0029);

        bus.register(0x0029, dispatcher).await;

        let msg = create_test_message(0x0029);
        bus.dispatch(msg).await.unwrap();

        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multiple_dispatchers() {
        let bus = MessageBus::new();
        let (dispatcher1, count1) = TestDispatcher::new(0x0029);
        let (dispatcher2, count2) = TestDispatcher::new(0x0029);
        let (dispatcher3, count3) = TestDispatcher::new(0x0029);

        bus.register(0x0029, dispatcher1).await;
        bus.register(0x0029, dispatcher2).await;
        bus.register(0x0029, dispatcher3).await;

        let msg = create_test_message(0x0029);
        bus.dispatch(msg).await.unwrap();

        // All three dispatchers should be invoked
        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
        assert_eq!(count3.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_unhandled_message() {
        let bus = MessageBus::new();
        let (dispatcher, _count) = TestDispatcher::new(0x0029);

        bus.register(0x0029, dispatcher).await;

        // Try to dispatch a different message type
        let msg = create_test_message(0x0030);
        let result = bus.dispatch(msg).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RadosError::Protocol(_)));
    }

    #[tokio::test]
    async fn test_dispatcher_error_propagation() {
        struct ErrorDispatcher;

        #[async_trait]
        impl Dispatcher for ErrorDispatcher {
            async fn dispatch(&self, _msg: Message) -> Result<(), RadosError> {
                Err(RadosError::Protocol("Test error".into()))
            }
        }

        let bus = MessageBus::new();
        bus.register(0x0029, Arc::new(ErrorDispatcher)).await;

        let msg = create_test_message(0x0029);
        let result = bus.dispatch(msg).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RadosError::Protocol(_)));
    }
}
