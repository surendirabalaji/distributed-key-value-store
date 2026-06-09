#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft) {}
  ~KvServer() = default;

  void on_apply(const rafty::ApplyResult &result) {
    auto decoded = decode_op(result.data);
    if (!decoded.has_value()) {
      store_applied_index_.store(result.index, std::memory_order_release);
      return;
    }

    const auto &op        = decoded->op;
    const auto &key       = decoded->key;
    const auto &value     = decoded->value;
    const uint64_t client_id = decoded->client_id;
    const uint64_t seq_num   = decoded->seq_num;

    auto &cs = client_shards_[client_id % N_CLIENT_SHARDS];

    kvpb::KvStatus final_status = kvpb::KV_SUCCESS;
    std::string    final_value;
    std::shared_ptr<PendingEntry> entry_ptr;
    bool is_duplicate;

    // Step 1: dedupe check + extract pending pointer (one lock acquisition).
    {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto dit = cs.dedupe.find(client_id);
      is_duplicate = (dit != cs.dedupe.end() && seq_num == dit->second.last_seq);
      if (is_duplicate) {
        final_status = dit->second.last_status;
        final_value  = dit->second.last_value;
      }
      PendingKey pkey{client_id, seq_num};
      auto pit = cs.pending.find(pkey);
      if (pit != cs.pending.end()) {
        entry_ptr = std::move(pit->second);
        cs.pending.erase(pit);
      }
    }

    // Step 2: KV mutation — no client-shard lock held.
    if (!is_duplicate) {
      auto &shard = shards_[shard_for(key)];
      std::lock_guard<std::mutex> slock(shard.mu);
      if (op == OpType::Put) {
        shard.data[key] = value;
      } else if (op == OpType::Append) {
        shard.data[key] += value;
      } else if (op == OpType::Get) {
        auto sit = shard.data.find(key);
        if (sit != shard.data.end()) final_value = sit->second;
      }
    }

    // Step 3: update dedupe after KV write.
    if (!is_duplicate) {
      std::lock_guard<std::mutex> lk(cs.mu);
      cs.dedupe[client_id] = ClientRecord{seq_num, final_status, final_value};
    }

    // Step 4: advance watermark after all state is consistent.
    store_applied_index_.store(result.index, std::memory_order_release);

    // Step 5: wake exactly one waiter via its private CV.
    if (entry_ptr) {
      std::lock_guard<std::mutex> elk(entry_ptr->mu);
      entry_ptr->status = final_status;
      entry_ptr->value  = std::move(final_value);
      entry_ptr->done   = true;
      entry_ptr->cv.notify_one();
    }
  }

  grpc::Status Put(grpc::ServerContext *context,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    (void)context;
    const std::string& key   = request->key();
    const std::string& value = request->value();
    const uint64_t client_id = request->client_id();
    const uint64_t seq_num   = request->seq_num();

    auto &cs = client_shards_[client_id % N_CLIENT_SHARDS];
    std::shared_ptr<PendingEntry> entry;
    {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto dit = cs.dedupe.find(client_id);
      if (dit != cs.dedupe.end() && seq_num == dit->second.last_seq) {
        response->set_status(dit->second.last_status);
        return grpc::Status::OK;
      }
      // Insert BEFORE propose() so on_apply always finds the entry.
      entry = std::make_shared<PendingEntry>();
      cs.pending[PendingKey{client_id, seq_num}] = entry;
    }

    const std::string op = encode_op(OpType::Put, key, value, client_id, seq_num);
    auto proposal = raft_.propose(op);
    if (!proposal.is_leader) {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto it = cs.pending.find(PendingKey{client_id, seq_num});
      if (it != cs.pending.end() && it->second == entry)
        cs.pending.erase(it);
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    kvpb::KvStatus status;
    {
      std::unique_lock<std::mutex> elk(entry->mu);
      bool ok = entry->cv.wait_for(elk, std::chrono::milliseconds(5000),
                                   [&]() { return entry->done; });
      status = entry->status;
      elk.unlock();  // Release before acquiring cs.mu (prevents lock-order inversion).
      if (!ok) {
        std::lock_guard<std::mutex> lk(cs.mu);
        auto it = cs.pending.find(PendingKey{client_id, seq_num});
        if (it != cs.pending.end() && it->second == entry)
          cs.pending.erase(it);
        response->set_status(kvpb::KV_TIMEOUT);
        return grpc::Status::OK;
      }
    }
    response->set_status(status);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *context,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    (void)context;
    const std::string& key   = request->key();
    const uint64_t client_id = request->client_id();
    const uint64_t seq_num   = request->seq_num();

    // Fast path: leader-lease read — zero global locks, no Raft round-trip.
    if (raft_.has_read_lease()) {
      const uint64_t read_ci = raft_.get_commit_index();
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
      while (store_applied_index_.load(std::memory_order_acquire) < read_ci) {
        if (std::chrono::steady_clock::now() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::microseconds(500));
      }
      if (store_applied_index_.load(std::memory_order_acquire) >= read_ci &&
          raft_.has_read_lease()) {
        auto &shard = shards_[shard_for(key)];
        std::string val;
        {
          std::lock_guard<std::mutex> slock(shard.mu);
          auto sit = shard.data.find(key);
          if (sit != shard.data.end()) val = sit->second;
        }
        response->set_status(kvpb::KV_SUCCESS);
        response->set_value(std::move(val));
        return grpc::Status::OK;
      }
    }

    // Slow path: Raft-log linearizable read.
    auto &cs = client_shards_[client_id % N_CLIENT_SHARDS];
    std::shared_ptr<PendingEntry> entry;
    {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto dit = cs.dedupe.find(client_id);
      if (dit != cs.dedupe.end() && seq_num == dit->second.last_seq) {
        response->set_status(dit->second.last_status);
        response->set_value(dit->second.last_value);
        return grpc::Status::OK;
      }
      entry = std::make_shared<PendingEntry>();
      cs.pending[PendingKey{client_id, seq_num}] = entry;
    }

    const std::string op = encode_op(OpType::Get, key, "", client_id, seq_num);
    auto proposal = raft_.propose(op);
    if (!proposal.is_leader) {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto it = cs.pending.find(PendingKey{client_id, seq_num});
      if (it != cs.pending.end() && it->second == entry)
        cs.pending.erase(it);
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    kvpb::KvStatus status;
    std::string val;
    {
      std::unique_lock<std::mutex> elk(entry->mu);
      bool ok = entry->cv.wait_for(elk, std::chrono::milliseconds(5000),
                                   [&]() { return entry->done; });
      status = entry->status;
      val    = std::move(entry->value);
      elk.unlock();
      if (!ok) {
        std::lock_guard<std::mutex> lk(cs.mu);
        auto it = cs.pending.find(PendingKey{client_id, seq_num});
        if (it != cs.pending.end() && it->second == entry)
          cs.pending.erase(it);
        response->set_status(kvpb::KV_TIMEOUT);
        return grpc::Status::OK;
      }
    }
    response->set_status(status);
    response->set_value(std::move(val));
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    (void)context;
    const std::string& key   = request->key();
    const std::string& value = request->value();
    const uint64_t client_id = request->client_id();
    const uint64_t seq_num   = request->seq_num();

    auto &cs = client_shards_[client_id % N_CLIENT_SHARDS];
    std::shared_ptr<PendingEntry> entry;
    {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto dit = cs.dedupe.find(client_id);
      if (dit != cs.dedupe.end() && seq_num == dit->second.last_seq) {
        response->set_status(dit->second.last_status);
        return grpc::Status::OK;
      }
      entry = std::make_shared<PendingEntry>();
      cs.pending[PendingKey{client_id, seq_num}] = entry;
    }

    const std::string op = encode_op(OpType::Append, key, value, client_id, seq_num);
    auto proposal = raft_.propose(op);
    if (!proposal.is_leader) {
      std::lock_guard<std::mutex> lk(cs.mu);
      auto it = cs.pending.find(PendingKey{client_id, seq_num});
      if (it != cs.pending.end() && it->second == entry)
        cs.pending.erase(it);
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    kvpb::KvStatus status;
    {
      std::unique_lock<std::mutex> elk(entry->mu);
      bool ok = entry->cv.wait_for(elk, std::chrono::milliseconds(5000),
                                   [&]() { return entry->done; });
      status = entry->status;
      elk.unlock();
      if (!ok) {
        std::lock_guard<std::mutex> lk(cs.mu);
        auto it = cs.pending.find(PendingKey{client_id, seq_num});
        if (it != cs.pending.end() && it->second == entry)
          cs.pending.erase(it);
        response->set_status(kvpb::KV_TIMEOUT);
        return grpc::Status::OK;
      }
    }
    response->set_status(status);
    return grpc::Status::OK;
  }

private:
  rafty::Raft &raft_;

  enum class OpType : uint8_t {
    Put    = 0,
    Append = 1,
    Get    = 2,
  };

  struct DecodedOp {
    OpType op;
    std::string key;
    std::string value;
    uint64_t client_id = 0;
    uint64_t seq_num   = 0;
  };

  // Binary encoding: [uint8_t op][uint64_t client_id][uint64_t seq_num]
  //                  [uint32_t key_len][uint32_t val_len][key bytes][val bytes]
  static constexpr size_t HEADER_SIZE = 1 + 8 + 8 + 4 + 4;

  static std::string encode_op(OpType op, const std::string &key,
                               const std::string &value, uint64_t client_id,
                               uint64_t seq_num) {
    const uint32_t kl = static_cast<uint32_t>(key.size());
    const uint32_t vl = static_cast<uint32_t>(value.size());
    std::string out;
    out.resize(HEADER_SIZE + kl + vl);
    char *h = out.data();
    h[0] = static_cast<char>(op);
    std::memcpy(h + 1,  &client_id, 8);
    std::memcpy(h + 9,  &seq_num,   8);
    std::memcpy(h + 17, &kl,        4);
    std::memcpy(h + 21, &vl,        4);
    if (kl) std::memcpy(h + HEADER_SIZE,      key.data(),   kl);
    if (vl) std::memcpy(h + HEADER_SIZE + kl, value.data(), vl);
    return out;
  }

  static std::optional<DecodedOp> decode_op(const std::string &raw) {
    if (raw.size() < HEADER_SIZE) return std::nullopt;
    const char *h = raw.data();
    const uint8_t op_v = static_cast<uint8_t>(h[0]);
    if (op_v > static_cast<uint8_t>(OpType::Get)) return std::nullopt;
    uint64_t client_id, seq_num;
    uint32_t kl, vl;
    std::memcpy(&client_id, h + 1,  8);
    std::memcpy(&seq_num,   h + 9,  8);
    std::memcpy(&kl,        h + 17, 4);
    std::memcpy(&vl,        h + 21, 4);
    if (HEADER_SIZE + kl + vl != raw.size()) return std::nullopt;
    DecodedOp out;
    out.op        = static_cast<OpType>(op_v);
    out.client_id = client_id;
    out.seq_num   = seq_num;
    out.key.assign(h + HEADER_SIZE,      kl);
    out.value.assign(h + HEADER_SIZE + kl, vl);
    return out;
  }

  struct ClientRecord {
    uint64_t last_seq          = 0;
    kvpb::KvStatus last_status = kvpb::KV_TIMEOUT;
    std::string last_value;
  };

  // Per-proposal notification primitive: one mutex + CV per in-flight RPC.
  // Avoids notify_all() waking all 64 waiters; only the single owning thread
  // waits here.
  struct PendingEntry {
    std::mutex             mu;
    std::condition_variable cv;
    bool                   done   = false;
    kvpb::KvStatus         status = kvpb::KV_TIMEOUT;
    std::string            value;
  };

  struct PendingKey {
    uint64_t client_id = 0;
    uint64_t seq_num   = 0;
    bool operator==(const PendingKey &o) const {
      return client_id == o.client_id && seq_num == o.seq_num;
    }
  };
  struct PendingKeyHash {
    size_t operator()(const PendingKey &k) const noexcept {
      const size_t h1 = std::hash<uint64_t>{}(k.client_id);
      const size_t h2 = std::hash<uint64_t>{}(k.seq_num);
      return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
  };

  // 16 client shards: each holds dedupe + pending maps for client_id % 16.
  // Eliminates the single global mu_ that was the primary contention point.
  static constexpr size_t N_CLIENT_SHARDS = 16;
  struct alignas(64) ClientShard {
    std::mutex mu;
    std::unordered_map<uint64_t, ClientRecord> dedupe;
    std::unordered_map<PendingKey, std::shared_ptr<PendingEntry>,
                       PendingKeyHash> pending;
  };
  std::array<ClientShard, N_CLIENT_SHARDS> client_shards_;

  // 16 KV data shards (unchanged from original).
  static constexpr size_t N_SHARDS = 16;
  struct StoreShard {
    std::unordered_map<std::string, std::string> data;
    mutable std::mutex mu;
  };
  std::array<StoreShard, N_SHARDS> shards_;

  size_t shard_for(const std::string &key) const noexcept {
    return std::hash<std::string>{}(key) % N_SHARDS;
  }

  // Highest index applied to shards_; read lock-free by lease-path Gets.
  alignas(64) std::atomic<uint64_t> store_applied_index_{0};
};

} // namespace kv
