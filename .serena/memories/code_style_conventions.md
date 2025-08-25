# Code Style and Conventions

## Rust Standards
- Use Rust 2021 edition
- Follow standard Rust naming conventions (snake_case for variables/functions, PascalCase for types)
- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting - must pass without warnings
- Prefer explicit error handling over unwrap()

## Project Structure
- Workspace-based architecture with multiple crates
- Crate organization: `denc` for encoding, `msgr2` for protocol, separate test crates
- Use workspace dependencies in Cargo.toml

## Error Handling
- Use `thiserror` for error types
- Prefer `Result<T, Error>` over panics
- Use `anyhow` for application-level error handling

## Async Programming
- Built on Tokio runtime
- Use async/await throughout
- Prefer structured concurrency

## Documentation
- Use Rust doc comments (`///`)
- Document public APIs
- Include examples where helpful