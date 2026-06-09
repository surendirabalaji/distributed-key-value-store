# TODO

Actionable implementation/debugging steps grouped by file.

## Current Tasks

### proto/kv.proto
- [x] Confirm request fields include key/value/client_id/seq_num.
- [x] Confirm return statuses are KV_SUCCESS/KV_NOTLEADER/KV_TIMEOUT.

### inc/kv/kv_server.hpp
- [x] Define server state for KV store, duplicate tracking, and pending waiters.
- [x] Define operation/result representation carried in Raft log.
- [x] Implement apply-side flow in on_apply(): parse, dedup, mutate state, notify waiters.
- [x] Implement Put flow: leadership gate, propose, wait for matching apply, timeout handling.
- [x] Implement Append flow: leadership gate, propose, wait for matching apply, timeout handling.
- [x] Implement Get flow with stale-read prevention via Raft-backed path.
- [x] Ensure duplicate requests return cached result rather than re-executing.

### src/raft.cpp and Raft API
- [x] Verify whether current propose path gives sufficient safety for stale-read prevention.
- [x] Decide whether propose_sync or additional leadership/commit checks are needed.

### integration_tests/kv_test.cpp
- [x] Map each test case to expected KV-server behavior and likely failure points.
- [x] Run kv_test and categorize failures by semantics (dedup, timeout, stale read, matching).

---
