# Task File: KV Service on Top of Raft

## Today’s task

Today we are working on implementing a **replicated key/value service** on top of the existing Raft implementation.

The goal is to build a KV server that provides **linearizability** for:

* `Put(key, value)`
* `Append(key, arg)`
* `Get(key)`

This KV layer must use Raft for replication and commitment. Servers must communicate only through the Raft log for KV operations.

---

## Target behavior

The system should behave like a single correct key/value store, even though multiple replicas exist.

### Required semantics

* `Put(key, value)` replaces the current value for the key.
* `Append(key, arg)` appends `arg` to the current value. If the key does not exist, it behaves like `Put`.
* `Get(key)` returns the current value, or `""` if the key does not exist.

### Correctness requirement

The service must be **linearizable**:

* if operations happen one at a time, behavior should match a single-copy KV store
* each operation must observe all completed operations before it
* no stale reads
* no duplicate execution for retried client requests

---

## Return codes

Each KV RPC must return one of:

* `KV_SUCCESS`
* `KV_NOTLEADER`
* `KV_TIMEOUT`

### Meaning

* `KV_SUCCESS`: operation completed correctly
* `KV_NOTLEADER`: this server is not the current Raft leader
* `KV_TIMEOUT`: operation could not be safely completed in time, such as when the leader loses majority or the operation does not commit

---

## Architecture

Each `kv_node` process runs three services:

* **Raft service** on port `P`
* **KV service** on port `P + 1000`
* **Tester service** on the tester port

Only the **KV service logic** is the task here.

### High-level request flow

1. KV server receives a client RPC: `Put`, `Append`, or `Get`
2. KV server serializes the operation into a form that can be placed in the Raft log
3. KV server calls `raft_.propose(...)`
4. If not leader, return `KV_NOTLEADER`
5. If leader, wait until the operation is committed and delivered through the Raft apply path
6. Apply the committed operation to the local in-memory KV state
7. Return the correct response to the client

### Important rule

There must be **no direct server-to-server KV synchronization** outside Raft.

All KV state changes must flow through the Raft log.

---

## Duplicate detection requirement

Clients may retry requests if they do not receive a response.

The server must implement **RIFL-style duplicate detection** so that the same operation is not executed twice.

### Request identity fields

Each request includes:

* `client_id`: unique 64-bit client ID
* `seq_num`: monotonically increasing sequence number per client

### Required behavior

For each client:

* track the latest processed sequence number
* cache the result for the latest unacknowledged request
* if a duplicate request arrives, return the cached result instead of executing again

This logic is especially important for `Put` and `Append`, but should be reasoned about carefully for `Get` as well.

---

## Stale read prevention

`Get()` must not return stale data.

If a server is no longer safely acting as part of the majority, it must not serve an old value and pretend success.

Instead, in such cases it should return:

* `KV_TIMEOUT`

This means reads must be tied to Raft leadership / commitment safety and must not bypass the replicated consistency model.

---

## Files relevant to this task

### Main file to implement

* `inc/kv/kv_server.hpp`

### May also require changes in Raft

* `inc/rafty/raft.hpp`
* `inc/rafty/impl/raft.ipp`

### Useful reference files

* `proto/kv.proto`
* `app/kv_node.cpp`
* `inc/kv/kv_client.hpp`
* `integration_tests/kv_test.cpp`

---

## Expected KV server responsibilities

The `KvServer` has access to:

* the Raft instance
* the `MessageQueue<ApplyResult>` ready queue

### On client RPC arrival

The KV server should:

1. Build an internal representation of the operation
2. Serialize it for the Raft log
3. Submit it with `raft_.propose(...)`
4. If propose indicates this node is not leader, return `KV_NOTLEADER`
5. Otherwise wait for the matching committed log entry to arrive through the apply path
6. Apply it to local KV state exactly once
7. Wake the waiting RPC handler and return the result

---

## Important implementation concerns

### 1. Operation representation

A committed Raft log entry must contain enough information to:

* identify operation type (`Put`, `Append`, `Get`)
* identify target key
* carry value/argument if needed
* carry `client_id`
* carry `seq_num`

### 2. Matching proposal to apply result

When a client RPC submits an operation through Raft, the KV layer must be able to match:

* the proposed request
* the applied committed result

This likely requires a coordination structure between:

* RPC handler threads
* the apply-consuming thread

Possible approaches include:

* per-index wait channels
* promises/futures
* condition variables with shared state

### 3. Exactly-once semantics

Even if the same request is committed or retried multiple times from the client perspective, the KV state machine must ensure duplicate suppression based on `(client_id, seq_num)`.

### 4. Read safety

A `Get` must not return a value based only on local memory if the leader may be stale.
The implementation should ensure the read is safe under Raft-based consistency expectations.

### 5. Timeout behavior

The server should not wait forever.
If the operation does not become safely committed in time, return `KV_TIMEOUT`.

---

## Suggested work order

1. Inspect `proto/kv.proto`

   * understand request/response fields
   * confirm status/value layout

2. Inspect `inc/kv/kv_server.hpp`

   * identify missing RPC handler logic
   * identify available members and helper structures

3. Inspect Raft `propose()` and apply path

   * verify what information is returned on propose
   * verify how committed entries are surfaced in `ApplyResult`

4. Design operation serialization and parsing

   * ensure all needed fields are preserved

5. Design in-memory state

   * KV map
   * duplicate detection cache
   * pending/waiting request tracking

6. Implement apply-side logic

   * parse committed command
   * perform deduplicated state transition
   * save result
   * notify waiter

7. Implement RPC handlers

   * build op
   * propose
   * wait
   * return status/result

8. Add timeout handling

   * ensure failed or stalled operations return `KV_TIMEOUT`

9. Revisit stale-read handling for `Get`

   * make sure reads cannot incorrectly succeed on a partitioned old leader

10. Run tests and debug failures

---

## Testing

Run:

```bash
cd ./build/integration_tests
./kv_test
```

Tests are expected to cover:

* basic `Put/Get/Append`
* concurrent clients
* leader failure and re-election
* partitions
* unreliable networks
* linearizability-related behavior

Hidden tests may also check correctness under more subtle failure conditions.

---

## Success criteria

The task is complete when:

* KV handlers are implemented correctly
* operations flow through Raft
* duplicate execution is prevented
* stale reads are prevented
* timeout behavior is correct
* tests in `kv_test` pass
* implementation remains consistent with Raft safety expectations

---

## Notes for today’s session

Focus on building the KV layer in a way that is:

* correct first
* easy to reason about
* aligned with Raft commit/apply semantics

Do not treat this as a standalone local KV store.
This is a replicated state machine problem.
