//! Integration tests for priority-based message queueing
//!
//! These tests verify that messages are sent in priority order,
//! ensuring that high-priority messages (like heartbeats) are not
//! delayed by bulk data transfers.

use bytes::Bytes;
use msgr2::message::{Message, MessagePriority, CEPH_MSG_PING};

#[test]
fn test_priority_queue_basic_ordering() {
    let mut queue = msgr2::protocol::PriorityQueue::new();

    // Simulate a scenario where we have bulk data operations
    // and heartbeat messages in the queue
    let bulk_msg1 = Message::new(100, Bytes::from("bulk data 1"));
    let bulk_msg2 = Message::new(101, Bytes::from("bulk data 2"));
    let heartbeat = Message::ping(); // Should be high priority
    let bulk_msg3 = Message::new(102, Bytes::from("bulk data 3"));

    // Add messages in a mixed order
    queue.push_back(bulk_msg1.clone());
    queue.push_back(bulk_msg2.clone());
    queue.push_back(heartbeat.clone());
    queue.push_back(bulk_msg3.clone());

    // Verify heartbeat comes out first despite being added third
    let first = queue.pop_front().unwrap();
    assert_eq!(first.msg_type(), CEPH_MSG_PING);
    assert_eq!(first.priority(), MessagePriority::High);

    // Verify bulk messages come out in FIFO order after heartbeat
    assert_eq!(queue.pop_front().unwrap().msg_type(), 100);
    assert_eq!(queue.pop_front().unwrap().msg_type(), 101);
    assert_eq!(queue.pop_front().unwrap().msg_type(), 102);
}

#[test]
fn test_priority_queue_mixed_priorities() {
    let mut queue = msgr2::protocol::PriorityQueue::new();

    // Create messages with all three priority levels
    let low_msg = Message::new(1, Bytes::from("low")).with_priority(MessagePriority::Low.to_u16());
    let normal_msg =
        Message::new(2, Bytes::from("normal")).with_priority(MessagePriority::Normal.to_u16());
    let high_msg =
        Message::new(3, Bytes::from("high")).with_priority(MessagePriority::High.to_u16());

    // Add in reverse priority order
    queue.push_back(low_msg);
    queue.push_back(normal_msg);
    queue.push_back(high_msg);

    // Verify they come out in correct priority order
    assert_eq!(queue.pop_front().unwrap().msg_type(), 3); // High first
    assert_eq!(queue.pop_front().unwrap().msg_type(), 2); // Normal second
    assert_eq!(queue.pop_front().unwrap().msg_type(), 1); // Low last
}

#[test]
fn test_priority_queue_maintains_fifo_within_priority() {
    let mut queue = msgr2::protocol::PriorityQueue::new();

    // Add multiple normal priority messages
    for i in 0..10 {
        let msg = Message::new(i, Bytes::from(format!("msg{}", i)))
            .with_priority(MessagePriority::Normal.to_u16());
        queue.push_back(msg);
    }

    // Add a high priority message in the middle
    let high_msg =
        Message::new(100, Bytes::from("urgent")).with_priority(MessagePriority::High.to_u16());
    queue.push_back(high_msg);

    // Add more normal priority messages
    for i in 10..20 {
        let msg = Message::new(i, Bytes::from(format!("msg{}", i)))
            .with_priority(MessagePriority::Normal.to_u16());
        queue.push_back(msg);
    }

    // High priority message should come out first
    assert_eq!(queue.pop_front().unwrap().msg_type(), 100);

    // Normal priority messages should maintain FIFO order
    for i in 0..20 {
        assert_eq!(queue.pop_front().unwrap().msg_type(), i);
    }
}

#[test]
fn test_priority_queue_iter_priority_order() {
    let mut queue = msgr2::protocol::PriorityQueue::new();

    // Add messages in various priorities
    queue.push_back(Message::new(1, Bytes::new()).with_priority(0)); // Low
    queue.push_back(Message::new(2, Bytes::new()).with_priority(1)); // Normal
    queue.push_back(Message::new(3, Bytes::new()).with_priority(2)); // High
    queue.push_back(Message::new(4, Bytes::new()).with_priority(1)); // Normal
    queue.push_back(Message::new(5, Bytes::new()).with_priority(2)); // High

    // Iterator should return messages in priority order
    let types: Vec<u16> = queue.iter().map(|m| m.msg_type()).collect();
    assert_eq!(types, vec![3, 5, 2, 4, 1]); // High, High, Normal, Normal, Low
}

#[test]
fn test_heartbeat_not_delayed_by_bulk() {
    let mut queue = msgr2::protocol::PriorityQueue::new();

    // Simulate 100 bulk data messages queued
    for i in 0..100 {
        let msg = Message::new(i, Bytes::from(vec![0u8; 1024]))
            .with_priority(MessagePriority::Normal.to_u16());
        queue.push_back(msg);
    }

    // Add a heartbeat message
    let heartbeat = Message::ping();
    queue.push_back(heartbeat);

    // Heartbeat should be at the front despite 100 messages ahead of it
    assert_eq!(queue.front().unwrap().msg_type(), CEPH_MSG_PING);
    assert_eq!(queue.front().unwrap().priority(), MessagePriority::High);

    // After popping heartbeat, bulk messages resume
    queue.pop_front();
    assert_eq!(queue.front().unwrap().msg_type(), 0);
}
