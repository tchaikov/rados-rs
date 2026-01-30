//! Example demonstrating message throttling and revocation
//!
//! This example shows how to:
//! 1. Use message throttling to control send rate
//! 2. Revoke messages that are no longer needed
//! 3. Handle timeouts with revocation
//! 4. Monitor throttle and revocation statistics

use msgr2::revocation::RevocationManager;
use msgr2::throttle::{MessageThrottle, ThrottleConfig};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== Message Throttling and Revocation Example ===\n");

    // Example 1: Basic throttling
    example_basic_throttling().await?;

    // Example 2: Message revocation
    example_message_revocation().await?;

    // Example 3: Timeout with revocation
    example_timeout_revocation().await?;

    // Example 4: Combined throttling and revocation
    example_combined().await?;

    Ok(())
}

/// Example 1: Basic message throttling
async fn example_basic_throttling() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 1: Basic Throttling ---");

    // Create throttle with 5 messages/sec and 1KB/sec limits
    let config = ThrottleConfig::with_limits(5, 1024, 10);
    let throttle = MessageThrottle::new(config);

    println!("Throttle config: 5 msg/sec, 1KB/sec, queue depth 10");

    // Send some messages
    for i in 0..8 {
        let msg_size = 200;

        // Check if we can send
        if throttle.try_send(msg_size).await {
            println!("  Message {}: Sending {} bytes", i, msg_size);
            throttle.record_send(msg_size).await;
        } else {
            println!("  Message {}: THROTTLED (waiting...)", i);
            throttle.wait_for_send(msg_size).await;
            println!("  Message {}: Sending {} bytes (after wait)", i, msg_size);
            throttle.record_send(msg_size).await;
        }
    }

    // Show statistics
    let stats = throttle.stats().await;
    println!("\nThrottle stats:");
    println!("  Total messages: {}", stats.total_messages);
    println!("  Total bytes: {}", stats.total_bytes);
    println!("  In-flight: {}", stats.in_flight_count);
    println!("  Recent messages: {}", stats.recent_messages);
    println!("  Recent bytes: {}", stats.recent_bytes);

    println!();
    Ok(())
}

/// Example 2: Message revocation
async fn example_message_revocation() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 2: Message Revocation ---");

    let manager = RevocationManager::new();

    // Register messages
    let (handle1, _rx1) = manager.register_message().await;
    let (handle2, mut rx2) = manager.register_message().await;
    let (handle3, _rx3) = manager.register_message().await;

    println!("Registered 3 messages");

    // Simulate sending message 1
    tokio::spawn({
        let manager = manager.clone();
        let id = handle1.id();
        async move {
            manager.mark_sending(id).await;
            sleep(Duration::from_millis(100)).await;
            manager.mark_sent(id).await;
            println!("  Message 1: Sent successfully");
        }
    });

    // Revoke message 2 before it's sent
    tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        let result = handle2.revoke().await;
        println!("  Message 2: Revoked (result: {:?})", result);
    });

    // Mark message 3 as sent
    manager.mark_sent(handle3.id()).await;
    println!("  Message 3: Sent immediately");

    // Wait for revocation signals
    sleep(Duration::from_millis(200)).await;

    // Check if message 2 was revoked
    match rx2.try_recv() {
        Ok(_) => println!("  Message 2: Revocation signal received"),
        Err(_) => println!("  Message 2: No revocation signal"),
    }

    // Show statistics
    let stats = manager.stats().await;
    println!("\nRevocation stats:");
    println!("  Total tracked: {}", stats.total);
    println!("  Queued: {}", stats.queued);
    println!("  Sending: {}", stats.sending);
    println!("  Sent: {}", stats.sent);
    println!("  Revoked: {}", stats.revoked);

    println!();
    Ok(())
}

/// Example 3: Timeout with revocation
async fn example_timeout_revocation() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 3: Timeout with Revocation ---");

    let manager = RevocationManager::new();

    // Register a message
    let (handle, _rx) = manager.register_message().await;
    let id = handle.id();

    println!("Registered message with 100ms timeout");

    // Simulate a slow send operation
    let send_task = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager.mark_sending(id).await;
            sleep(Duration::from_millis(200)).await; // Slow send
            manager.mark_sent(id).await;
            println!("  Send completed (took 200ms)");
        }
    });

    // Wait with timeout
    let timeout_result =
        tokio::time::timeout(Duration::from_millis(100), handle.wait_completion()).await;

    match timeout_result {
        Ok(status) => {
            println!("  Message completed: {:?}", status);
        }
        Err(_) => {
            println!("  Timeout! Message is still in progress");
            // In a real scenario, you might revoke the message here
        }
    }

    // Wait for send task to complete
    send_task.await?;

    println!();
    Ok(())
}

/// Example 4: Combined throttling and revocation
async fn example_combined() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 4: Combined Throttling and Revocation ---");

    // Create throttle with strict limits
    let config = ThrottleConfig::with_limits(3, 500, 5);
    let throttle = MessageThrottle::new(config);

    // Create revocation manager
    let manager = RevocationManager::new();

    println!("Throttle: 3 msg/sec, 500 bytes/sec, queue depth 5");
    println!("Sending 10 messages with revocation support\n");

    let mut handles = Vec::new();

    // Send messages with throttling and revocation support
    for i in 0..10 {
        let msg_size = 100;

        // Wait for throttle permission
        throttle.wait_for_send(msg_size).await;

        // Register for revocation
        let (handle, revocation_rx) = manager.register_message().await;
        let id = handle.id();

        println!("  Message {}: Queued (id: {:?})", i, id);

        // Simulate sending with revocation support
        let throttle_clone = throttle.clone();
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = revocation_rx => {
                    println!("    Message {:?}: REVOKED during send", id);
                }
                _ = async {
                    manager_clone.mark_sending(id).await;
                    sleep(Duration::from_millis(50)).await;
                    manager_clone.mark_sent(id).await;
                    throttle_clone.record_send(msg_size).await;
                    println!("    Message {:?}: Sent successfully", id);
                } => {}
            }
        });

        handles.push(handle);

        // Revoke every 3rd message
        if i % 3 == 2 {
            if let Some(handle) = handles.pop() {
                let id = handle.id();
                tokio::spawn(async move {
                    sleep(Duration::from_millis(25)).await;
                    let result = handle.revoke().await;
                    println!("    Message {:?}: Revoke attempt = {:?}", id, result);
                });
            }
        }
    }

    // Wait for all operations to complete
    sleep(Duration::from_millis(500)).await;

    // Show final statistics
    println!("\nFinal Statistics:");

    let throttle_stats = throttle.stats().await;
    println!("  Throttle:");
    println!("    Total messages: {}", throttle_stats.total_messages);
    println!("    Total bytes: {}", throttle_stats.total_bytes);
    println!("    In-flight: {}", throttle_stats.in_flight_count);

    let revocation_stats = manager.stats().await;
    println!("  Revocation:");
    println!("    Total tracked: {}", revocation_stats.total);
    println!("    Sent: {}", revocation_stats.sent);
    println!("    Revoked: {}", revocation_stats.revoked);

    println!();
    Ok(())
}
