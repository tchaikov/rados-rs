# Task Completion Checklist

When completing any task, ensure:

## Code Quality
1. `cargo fmt` - Code is properly formatted
2. `cargo clippy` - No clippy warnings 
3. `cargo build` - Code compiles without warnings
4. `cargo test` - All tests pass

## Testing
- Test against local Ceph cluster when applicable
- Use `RUST_LOG=debug` for debugging
- Verify behavior matches official Ceph client when possible

## Documentation
- Update relevant documentation
- Add inline comments for complex logic
- Update TODO.md if applicable

## Git
- Use descriptive commit messages
- Follow conventional commits if possible
- Only commit when explicitly asked by user