# KV Put Request Flows (Raft + Promise/Future + Dedup)

Date: 2026-04-12

This document captures the expected end-to-end behavior for `Put(x, 1)` across leader changes, timeouts, and retries.

---

## Flow 1: Happy Path (Leader commits own proposal)

### Narrative
1. Client sends `Put(x, 1)` to leader A.
2. RPC handler thread:
   - Serializes request as `"Put x 1 42 5"`.
   - Creates a promise and stores `pending[7] = promise`.
   - Calls `propose("Put x 1 42 5")` and gets log index `7`.
   - Blocks on `future.get()` with timeout.
3. Raft replicates to majority and commits index `7`.
4. Apply thread handles index `7`:
   - Dedup check for `(42, 5)`:
     - Not seen before → execute `Put(x, 1)`, update KV map, cache result.
   - Pending guard check:
     - `pending.count(7) > 0` → `set_value(KV_SUCCESS)`, erase `pending[7]`.
5. RPC handler wakes up and returns `KV_SUCCESS` to client.


---

## Flow 2: Timeout + Leadership Change + Safe Guarding

### Narrative
1. Client sends `Put(x, 1)` to leader A.
2. RPC handler creates `pending[7]` and blocks on `future.get()`.
3. A gets partitioned; B becomes new leader.
   - B commits a different entry at index `7`.
   - A’s original entry never commits.
4. RPC handler on A times out:
   - Erases `pending[7]`.
   - Returns `KV_TIMEOUT` to client.
5. Apply thread later processes index `7` (different entry):
   - Guard check `pending.count(7) > 0` fails.
   - Handler safely skips promise fulfillment (no crash / no invalid access).
6. Client retries on B and succeeds.
```

---

## Flow 3: Duplicate Retry After Lost Response (Idempotency)

### Narrative
1. Client sends `Put(x, 1)` with request id `(42, 5)` to leader A.
   - A proposes, commits, and applies.
   - Response is lost in transit.
2. Client retries the same `(42, 5)` to leader B.
   - B proposes and commits (for example at index `12`).
3. Apply thread processes index `12` on B:
   - Dedup table check for `(42, 5)` is positive.
   - Return cached result and skip re-execution.
4. B responds `KV_SUCCESS` to client.
   - No double apply.

---

## Core Safety Rules (Implementation Checklist)

- Always guard pending promise completion with `pending.count(index) > 0`.
- Always erase `pending[index]` on timeout paths.
- Dedup key `(client_id, request_id)` must be checked before state machine execution.
- Cache and return prior result for duplicate requests.
- Never assume the committed entry at a log index is the same command originally proposed by that RPC handler.
