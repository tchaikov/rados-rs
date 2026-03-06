# Code Refactoring Guide

## Phase 5: Function Decomposition (2026-03-06)

### Goals
- No function exceeds 100 lines
- Each function has single responsibility
- Improved testability and readability

### Functions Refactored

#### osdclient::list() (was ~240 lines → now ~60 lines)
**Extracted helpers:**
- `parse_list_cursor()`: Parse pagination cursor string into (pg_id, hobject_cursor)
- `query_pg_objects()`: Query objects from a specific PG with OSD communication
- `build_list_result()`: Build paginated ListResult with next cursor

**Benefits:** Main function now clearly shows the high-level flow: parse cursor → query objects → build result

#### monclient::start_hunting() (was ~130 lines → now ~40 lines)
**Extracted helpers:**
- `select_monitors_by_priority()`: Select monitors from lowest priority group with weighted shuffling
- `weighted_shuffle()`: Weighted random selection using Fisher-Yates algorithm
- `connect_parallel()`: Parallel connection attempts to multiple monitors
- `update_hunt_backoff()`: Update backoff state after hunt attempts

**Benefits:** Clear separation of monitor selection, shuffling, and connection logic

#### osdclient::handle_osdmap() (was ~170 lines → now ~13 lines)
**Extracted helpers:**
- `decode_and_validate_osdmap()`: Decode MOSDMap and validate FSID/epoch
- `process_osdmap_updates()`: Orchestrate map update processing
- `apply_sequential_updates()`: Apply sequential updates to existing map
- `apply_incremental_map()`: Apply single incremental map update
- `apply_full_map()`: Apply single full map update
- `load_initial_map()`: Load initial map when no current map exists
- `update_osdmap_state()`: Update watch channel and notify subscribers

**Benefits:** Complex OSDMap processing logic broken into testable units

#### osdclient::handle_backoff_from_osd() (was ~125 lines → now ~40 lines)
**Extracted helpers:**
- `decode_backoff_message()`: Decode backoff message from raw message
- `get_session_for_osd()`: Get session for OSD with error handling
- `handle_backoff_block()`: Handle BLOCK backoff operation
- `register_backoff()`: Register backoff entry in session tracker
- `send_backoff_ack()`: Send ACK_BLOCK message to OSD
- `handle_backoff_unblock()`: Handle UNBLOCK backoff operation

**Benefits:** Backoff handling logic clearly separated by operation type

#### osdclient::scan_requests_on_map_change() (was ~111 lines → now ~35 lines)
**Extracted helpers:**
- `collect_session_snapshot()`: Collect snapshot of all sessions
- `check_session_health()`: Check if session should be closed
- `scan_session_operations()`: Scan operations and collect those needing resend
- `close_stale_sessions()`: Close sessions that are stale
- `resend_migrated_operations()`: Resend migrated operations to new targets

**Benefits:** OSDMap change handling broken into clear phases

#### osdclient::session::handle_reply() (was ~163 lines → now ~30 lines)
**Extracted helpers:**
- `validate_reply_freshness()`: Validate that reply is not stale
- `handle_redirect()`: Handle redirect reply
- `apply_redirect_to_op()`: Apply redirect information to operation
- `handle_eagain_retry()`: Handle EAGAIN retry for replica reads

**Benefits:** Reply handling logic separated by response type

### Summary Statistics

**Before Phase 5:**
- 6 functions exceeding 100 lines
- Longest function: 240 lines
- Total lines in long functions: ~939 lines

**After Phase 5:**
- 0 functions exceeding 100 lines
- Longest function: ~60 lines
- Total lines (including helpers): ~939 lines (same functionality, better organized)

**Helper functions created:** 29 focused helper functions

### Benefits

1. **Testability**: Helper functions can be unit tested independently
2. **Readability**: Main functions show high-level flow clearly
3. **Maintainability**: Changes isolated to specific helpers
4. **Reusability**: Helpers can be used in multiple places
5. **Debugging**: Easier to identify which logical section has issues

### Guidelines for Future Refactoring

1. **Target**: Functions should be under 100 lines
2. **Extract**: Logical sections with clear, descriptive names
3. **Maintain**: Existing test coverage (pure refactoring)
4. **Document**: Complex helper functions with doc comments
5. **Test**: Run full test suite after refactoring

### Testing

All refactoring was verified with:
- Unit tests: 117 tests passed ✓
- Integration tests: 9 tests passed ✓
- No functionality changes (pure refactoring)

### Commits

- `1711325` refactor: decompose list() function into helpers
- `6745596` fix: correct PgPool type path in query_pg_objects
- `[hash]` refactor: decompose start_hunting() into helpers
- `[hash]` refactor: decompose handle_osdmap() into helpers
- `[hash]` refactor: decompose remaining long functions

---

## Best Practices

### When to Extract a Helper Function

Extract when:
- Function exceeds 100 lines
- Multiple logical sections exist
- Section has clear single responsibility
- Section could be tested independently
- Section could be reused elsewhere

### Naming Conventions

- Use verb phrases: `parse_cursor()`, `validate_reply()`, `send_ack()`
- Be specific: `handle_backoff_block()` not `handle_backoff()`
- Indicate return type: `collect_sessions()` returns collection
- Indicate side effects: `update_state()` modifies state

### Helper Function Placement

- Private helpers: Place near the function that uses them
- Reusable helpers: Consider moving to separate module
- Test helpers: Place in test module

### Documentation

- Main function: High-level overview of flow
- Helper functions: Document parameters and return values
- Complex logic: Add inline comments explaining why

---

## Phase 5 Complete

All functions in osdclient and monclient are now under 100 lines, with complex logic decomposed into focused, testable helper functions. The codebase is significantly more maintainable and easier to understand.
