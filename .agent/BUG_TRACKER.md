# BUG TRACKER

Track bugs discovered during debugging and review.

## Template
- ID:
- Title:
- Status: OPEN
- Files:
- Area:
- Description:
- Failing scenario:
- Expected:
- Actual:
- Suspected cause:

---

## BUG-001
- ID: BUG-001
- Title: Append value loses spaces due to parser format
- Status: CLOSED
- Files: inc/kv/kv_server.hpp, integration_tests/kv_test.cpp
- Area: KV operation serialization/parsing
- Description: KV operations are serialized as space-delimited text and parsed with stream extraction operators. This drops leading/trailing spaces in values.
- Failing scenario: BasicOpsC appends " world" to "hello".
- Expected: Get returns "hello world".
- Actual: Get returns "helloworld".
- Suspected cause: `istringstream >> value` tokenizes on whitespace, so leading space is lost.
- Resolution: Switched to a length-delimited encoding (header lines + raw key/value bytes) so payloads preserve whitespace; verified with integration_tests/kv_test.cpp (BasicOpsC passes).

## BUG-002
- ID: BUG-002
- Title: Concurrent append tokenization breaks linearizable value format
- Status: CLOSED
- Files: inc/kv/kv_server.hpp, integration_tests/kv_test.cpp
- Area: Append command encoding robustness
- Description: Concurrent append payloads containing spaces are parsed as single tokens, collapsing separators and causing wrong final concatenated string shape.
- Failing scenario: ConcurrentAppendC expects 30 space-separated tokens after many appends.
- Expected: Value contains all appended tokens separated as sent.
- Actual: Token count is 1.
- Suspected cause: Space-delimited log command format cannot preserve arbitrary string payloads.
- Resolution: Same length-delimited encoding as BUG-001; verified with integration_tests/kv_test.cpp (ConcurrentAppendC passes).

---
