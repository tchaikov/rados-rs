// Simple test to verify denc integration works
use denc::EntityAddr;
use std::net::SocketAddr;

fn main() {
    println!("Testing denc EntityAddr integration...");
    
    let addr: SocketAddr = "127.0.0.1:6789".parse().unwrap();
    let entity_addr = EntityAddr::new(addr, 12345);
    
    println!("Created EntityAddr: {}", entity_addr);
    println!("✓ Basic denc integration works!");
}