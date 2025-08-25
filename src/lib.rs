pub mod auth;
pub mod config;
pub mod error;
pub mod messenger;
pub mod types;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
