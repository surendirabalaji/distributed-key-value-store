#pragma once

#include <cstdint>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <optional>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"
#include <random>
#include <vector>
#include <map>
// it will pick up correct header
// when you generate the grpc proto files
#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

enum class Role {
  Follower,
  Candidate,
  Leader,
};

class Raft {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run(); /* lab 1 */
  ProposalResult propose(const std::string &data); /* lab 1 */
  State get_state() const; /* lab 2 */

  // lab3: sync propose
  ProposalResult propose_sync(const std::string &data);

  // lab3: leader lease reads — lock-free queries for KV fast path
  bool has_read_lease() const;
  uint64_t get_commit_index() const;
  uint64_t get_last_applied() const;

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

  class RaftRpcImpl final : public raftpb::RaftService::Service {
  public:
    explicit RaftRpcImpl(Raft &raft) : raft_(raft) {}

    grpc::Status AppendEntries(grpc::ServerContext *context,
                               const raftpb::AppendEntriesRequest *request,
                               raftpb::AppendEntriesReply *reply) override;

    grpc::Status RequestVote(grpc::ServerContext *context,
                             const raftpb::RequestVoteRequest *request,
                             raftpb::RequestVoteReply *reply) override;

  private:
    Raft &raft_;
  };

private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);

  void applier_loop();

  // Part B functions
  void ticker();
  void reset_election_timer();
  bool election_timeout_elapsed() const;
  int get_random_election_timeout() const;
  void send_heartbeats();
  void become_follower(uint64_t term);
  void become_candidate();
  void become_leader();
  void start_election();

  // Part C functions (declared now, implemented later)
  void request_vote_from_peer(uint64_t peer_id, uint64_t election_term);
  bool is_log_up_to_date(uint64_t last_log_term, uint64_t last_log_index) const;

protected:
  // WARN: do not modify `mtx` and `logger`.
  mutable std::mutex mtx;
  std::unique_ptr<rafty::utils::logger> logger;

private:
  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<RaftRpcImpl> rpc_service_;

  // Lab 1 state
  uint64_t current_term;
  std::optional<uint64_t> voted_for;
  std::atomic<Role> role;
  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_time;
  std::chrono::milliseconds election_timeout;
    // Threading
  std::unique_ptr<std::thread> ticker_thread_;
  std::atomic<bool> ticker_running_;

  std::unique_ptr<std::thread> applier_thread_;
  std::atomic<bool> applier_running_;
  std::condition_variable applier_cv_;

  // Election state
  std::unordered_set<uint64_t> votes_received_;

  // Log
  std::vector<raftpb::Entry> log_;

  // Volatile state (atomic for lock-free reads on Get fast path)
  std::atomic<uint64_t> commit_index_;
  std::atomic<uint64_t> last_applied_;

  // Leader state
  std::unordered_map<uint64_t, uint64_t> next_index_;
  std::unordered_map<uint64_t, uint64_t> match_index_;

  // Batching v2: true while ≥1 peer thread from the current round is live.
  // Accessed only under mtx — plain bool, no atomic needed.
  bool repl_in_flight_{false};

  // Batching v1: time-window accumulation (complements v2).
  // A short window prevents redundant rounds when proposals burst immediately
  // after a flush.  Both fields accessed only under mtx.
  bool has_pending_proposals_{false};
  int64_t last_flush_time_ns_{0};  // steady_clock nanoseconds at last send_heartbeats()
  static constexpr int64_t BATCH_WINDOW_NS = 200'000;  // 200 µs

  // Constants
  static constexpr int HEARTBEAT_INTERVAL_MS = 50;
  static constexpr int MIN_ELECTION_TIMEOUT_MS = 150;
  static constexpr int MAX_ELECTION_TIMEOUT_MS = 300;

  // Leader lease state (lock-free)
  std::atomic<int64_t> last_majority_contact_ns_{0};  // 0 = no lease
  static constexpr int64_t LEASE_DURATION_NS = 135'000'000;  // 135ms = 0.9 × MIN_ELECTION_TIMEOUT_MS
  // Guards lease reads: true once this leader-tenure has committed an entry
  // in the current term, ensuring commit_index_ is accurate.
  std::atomic<bool> has_committed_current_term_{false};

  static_assert(std::atomic<int64_t>::is_always_lock_free);
  static_assert(std::atomic<uint64_t>::is_always_lock_free);

};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
