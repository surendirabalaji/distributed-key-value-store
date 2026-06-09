# Rafty: Distributed Key-Value Store with Raft Consensus

A production-ready C++ implementation of a distributed key-value store built on the **Raft consensus algorithm**. This project demonstrates advanced distributed systems concepts including consensus protocols, state machine replication, client deduplication, and fault tolerance.

## Overview

Rafty is a high-performance distributed key-value store that guarantees strong consistency across replicated nodes using the Raft consensus protocol. The implementation includes sophisticated features such as:

- **Raft Consensus Algorithm**: Full implementation of the Raft protocol for leader election and log replication
- **gRPC Communication**: Efficient RPC-based inter-node communication using Protocol Buffers
- **Client Idempotency**: RIFL (Request-ID, Flat-Locking) semantics for exactly-once semantics despite network failures
- **State Machine Replication**: Ordered log application with deduplication and promise-based asynchronous RPC handling
- **Comprehensive Testing**: Integration tests and unit tests for all consensus mechanisms
- **OpenTelemetry Tracing**: Optional distributed tracing support for observability (OTEL)

## Architecture

### Core Components

| Component | Purpose |
|-----------|---------|
| **Raft Consensus** (`src/raft.cpp`) | Leader election, log replication, commit index tracking |
| **KV Service** (`inc/kv/`) | Key-value state machine with put/get/append operations |
| **gRPC Handlers** | AppendEntries, RequestVote, and client request handlers |
| **Promise/Future** | Asynchronous request handling with timeout support |
| **Deduplication** | Client request tracking to ensure idempotency |

### Key Features

#### 1. **Raft Protocol Implementation**
- Leader election with randomized timeouts
- Parallel log replication to followers
- Automatic leader detection and failover
- State persistence across restarts

#### 2. **Client Deduplication (RIFL)**
- Prevents duplicate command execution after network failures
- Tracks client IDs and sequence numbers
- Safe response caching for retries

#### 3. **Promise-Based Async Handling**
- Non-blocking request processing with configurable timeouts
- Automatic cleanup on leadership changes
- Safe deferred promise fulfillment

## Prerequisites

- **CMake** >= 3.22.1
- **G++** >= 13.1.0
- **C++20** standard support
- macOS or Linux (tested on Ubuntu 24.04.4 LTS)

## Build Instructions

### 1. Install Dependencies

```bash
./setup.sh
```

This script installs required libraries:
- **gRPC** and Protocol Buffers
- **GoogleTest** for unit testing
- **spdlog** for structured logging

### 2. Configure and Build

```bash
mkdir -p build
cd build
cmake ..
make -j$(nproc)
```

### 3. Build Artifacts

After successful build, executables are available in `build/app/`:
- `kv_node` - Standalone KV store node
- `raft_node` - Raft consensus benchmark
- `multinode` - Multi-node cluster simulation
- `latency` / `tput` - Performance benchmarking tools

## Usage

### Single Node

```bash
./build/app/kv_node --port 50051
```

### Multi-Node Cluster

```bash
# Terminal 1: Node 1
./build/app/multinode --id 1 --port 50051 --peers localhost:50052,localhost:50053

# Terminal 2: Node 2
./build/app/multinode --id 2 --port 50052 --peers localhost:50051,localhost:50053

# Terminal 3: Node 3
./build/app/multinode --id 3 --port 50053 --peers localhost:50051,localhost:50052
```

## API Reference

### KV Service (gRPC)

```protobuf
service KvService {
  rpc Put(PutRequest) returns (KvResponse) {}
  rpc Get(GetRequest) returns (GetResponse) {}
  rpc Append(AppendRequest) returns (KvResponse) {}
}
```

**Put**: Insert or overwrite a key-value pair
```
PutRequest { key, value, client_id, seq_num }
→ KvResponse { status }
```

**Get**: Retrieve value for a key (forwards to leader)
```
GetRequest { key, client_id, seq_num }
→ GetResponse { status, value }
```

**Append**: Atomically append to value (list semantics)
```
AppendRequest { key, value, client_id, seq_num }
→ KvResponse { status }
```

## Testing

### Run All Tests

```bash
cd build
ctest
```

### Run Specific Test Suite

```bash
./integration_tests/kv_test          # KV functionality
./integration_tests/raft_test         # Raft protocol
./integration_tests/extra_cases      # Edge cases
```

## Performance Benchmarks

The `bench/` directory contains performance analysis:
- `lat-tput.py` - Latency vs throughput analysis
- `result_*.txt` - Benchmark results for different workloads (50/50, 90/10 read/write)
- `baseline.md` - Performance baseline documentation

### Run Benchmarks

```bash
./build/app/tput --duration 60 --threads 16
./build/app/latency --samples 1000
```

## Project Structure

```
├── src/                    # Core implementation
│   └── raft.cpp           # Raft protocol logic
├── inc/                   # Public headers
│   ├── rafty/            # Raft interfaces
│   ├── kv/               # KV state machine
│   └── common/           # Shared utilities
├── proto/                 # Protocol Buffer definitions
│   ├── kv.proto          # KV service RPC
│   ├── raft.proto        # Raft service RPC
│   └── tester.proto      # Testing utilities
├── app/                  # Executable implementations
│   ├── kv_node.cpp       # Standalone node
│   ├── multinode.cpp     # Multi-node cluster
│   ├── raft_node.cpp     # Raft benchmark
│   └── bench_common.hpp  # Benchmarking utilities
├── integration_tests/    # End-to-end tests
├── unittests/           # Unit tests
├── build/               # Build output (generated)
└── bench/               # Benchmark results
```

## Key Implementation Details

### Raft State Machine Transitions

1. **Startup**: Every node starts as a follower
2. **Election Timeout**: Follower becomes candidate, requests votes
3. **Vote Majority**: Candidate becomes leader
4. **Heartbeats**: Leader sends periodic AppendEntries to maintain authority
5. **Higher Term Detected**: Any node steps down to follower

### Request Flow (Put Operation)

1. Client sends `Put(key, value, client_id, seq_num)` to any node
2. If not leader, node forwards to leader
3. Leader appends entry to its log
4. Leader replicates to majority of followers
5. Once majority acks, leader commits the entry
6. Entry is applied to state machine
7. Deduplication check ensures idempotency
8. Response returned to client via Promise/Future

### Fault Tolerance

- **Partition Tolerance**: Nodes in minority partition reject requests gracefully
- **Leader Failure**: Followers detect via election timeout, new leader elected
- **Network Delays**: Timeouts and retries handle transient failures
- **Duplicate Requests**: RIFL deduplication prevents re-execution after retries

## Build Configuration

### Debug Build (Default)
```bash
cmake .. -DCMAKE_BUILD_TYPE=Debug
```

### Release Build (Optimized)
```bash
cmake .. -DCMAKE_BUILD_TYPE=Release
```

### With OpenTelemetry Tracing
```bash
cmake .. -DTRACING=ON
```

## Technical Highlights

- **C++20** with no extensions for maximum portability
- **Thread-safe** log replication with fine-grained locking
- **Lock-free** data structures where applicable
- **gRPC** for efficient binary RPC communication
- **Promise/Future** pattern for async-await semantics
- **Structured logging** with spdlog for debugging

## Deliverables & Verification

- ✅ Full Raft protocol implementation with leader election
- ✅ Log replication and consistency verification
- ✅ Client-side deduplication with RIFL semantics
- ✅ Promise-based async RPC handling with timeouts
- ✅ Comprehensive integration and unit tests
- ✅ Performance benchmarking suite
- ✅ OpenTelemetry observability support

## Contributing

This project is a distributed systems educational implementation. For questions or improvements, please refer to the implementation documentation in `doc/`.

## License

Educational implementation for distributed systems coursework.

---

**Authors**: Surendira Balaji, Karthikeyan(https://github.com/Karthikeyan1206)

