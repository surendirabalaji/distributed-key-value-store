# Q&A LOG

Record important questions and guidance.

## Entries
- Timestamp: 2026-04-13
- Question: "done with the task can you check the code and run the build , run testing with test cases. Fix the failure test cases. Also make sure it doesnt fail for any hidden test cases or any failure scenarios"
- Answer summary: Reviewed kv_server implementation, executed kv_test from build output, found 2 failing tests (BasicOpsC and ConcurrentAppendC), and traced failures to whitespace-unsafe command serialization/parsing.
- Files referenced: inc/kv/kv_server.hpp; integration_tests/kv_test.cpp; .agent/BUG_TRACKER.md
- Next step: Replace space-delimited command encoding with robust serialization that preserves full string payloads and re-run tests.

- Timestamp: 2026-04-12
- Question: "can we go step by step as defined in todo. Lets go one by one. Guide me to do them. Give me code reference. explain me the syntax"
- Answer summary: Began strict one-by-one walkthrough from TODO; starting with KvServer state definition and member/lifetime syntax before implementation.
- Files referenced: .agent/TODO.md; inc/kv/kv_server.hpp; inc/common/common.hpp
- Next step: move `store_` to class member state and define dedup/pending state structures as private members.

- Timestamp: 2026-04-12
- Question: "1. Class params needs to be initialised like key, value store, dedupe table, waiter thread 2. Next we are gonna implement put. I will define the logic in next prompt 3. test the flow"
- Answer summary: Confirmed plan. First complete internal KvServer state initialization and invariants before Put logic. Then implement Put and run kv_test flow checks.
- Files referenced: inc/kv/kv_server.hpp; integration_tests/kv_test.cpp
- Next step: finalize data structure contracts and thread/waiter lifecycle rules.

- Timestamp: 2026-04-12
- Question: "ok lets go step by step. Guide me to do the first step"
- Answer summary: Started step-by-step mentoring. First step is to design the operation/result model and shared state in kv_server before writing logic.
- Files referenced: inc/kv/kv_server.hpp; proto/kv.proto; src/raft.cpp
- Next step: Define exact operation fields, dedup cache shape, and pending-wait mapping.

- Timestamp: 2026-04-12
- Question: "Today we are gonna work on the task described in the file. You should work as instructed in the instructions file"
- Answer summary: Confirmed mentor/debugger mode, inspected task and core files, and started a concrete file-by-file implementation/debug plan without code output.
- Files referenced: .agent/task.md; .agent/instructions_agent_suren.md; inc/kv/kv_server.hpp; proto/kv.proto; src/raft.cpp; integration_tests/kv_test.cpp
- Next step: Walk through kv_server.hpp data/control flow design before implementation.

---
