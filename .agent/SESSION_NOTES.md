# SESSION NOTES

## Current goal
- Implement KV service behavior on top of Raft in inc/kv/kv_server.hpp with linearizable Put/Get/Append.

## Current file
- inc/kv/kv_server.hpp (initial state setup + Put flow next)

## Blockers
- kv_server.hpp currently has TODO stubs for all RPC handlers and apply path.
- Need clear operation encoding/decoding format for Raft log command strings.
- Need robust wait/notify mapping from proposed log index to applied result.

## Next step
- Initialize KvServer internal state groups (store, dedup, waiters), then implement Put path and validate with kv tests.

---
