#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)), id(config.id), listening_addr(config.addr),
      peer_addrs(config.peer_addrs), dead(false), ready_queue(ready),  current_term(0),          
      voted_for(std::nullopt),  
      role(Role::Follower),      
      ticker_running_(false),
      applier_running_(false),
      commit_index_(0),          
      last_applied_(0)  
  // TODO: add more field if desired
{
    // Dummy log entry at index 0
  raftpb::Entry dummy;
  dummy.set_term(0);
  dummy.set_index(0);
  log_.push_back(dummy);

  election_timeout = std::chrono::milliseconds(get_random_election_timeout());
  last_heartbeat_time = std::chrono::steady_clock::now();

  logger->info("Node {} initialized, timeout={}ms", id, election_timeout.count());
}

Raft::~Raft() {
  this->kill();  //  Stop ticker first
  if (ticker_thread_ && ticker_thread_->joinable()) {
    ticker_thread_->join();  //  Wait for it
  }
  if (applier_thread_ && applier_thread_->joinable()) {
    applier_thread_->join();
  }
  this->stop_server();
}

void Raft::run() {
  ticker_running_ = true;
  ticker_thread_ = std::make_unique<std::thread>(&Raft::ticker, this);

  applier_running_ = true;
  applier_thread_ = std::make_unique<std::thread>(&Raft::applier_loop, this);
  logger->info("Raft node {} started", id);
}

State Raft::get_state() const {
  std::lock_guard<std::mutex> lock(mtx);
  State state;
  state.term = current_term;
  state.is_leader = (role == Role::Leader);
  return state;
}

ProposalResult Raft::propose(const std::string &data) {
  std::lock_guard<std::mutex> lock(mtx);
  
  ProposalResult result;
  result.is_leader = (role == Role::Leader);
  
  if (role != Role::Leader) {
    result.index = 0;
    result.term = 0;
    return result;
  }
  
  // Create new log entry
  raftpb::Entry entry;
  entry.set_term(current_term);
  entry.set_index(log_.size());
  entry.set_command(data);
  
  log_.push_back(entry);
  
  result.index = entry.index();
  result.term = entry.term();
  
  logger->info("Node {} (LEADER) received proposal, appended at index={} term={}",
               id, result.index, result.term);

  if (!repl_in_flight_) {
    send_heartbeats();   // v2: fire immediately; concurrent proposals batch via finish() chain
  }
  // if repl_in_flight_: v2 batching path — finish() chains a new round

  return result;
}

ProposalResult Raft::propose_sync(const std::string &data) {
  auto result = propose(data);
  if (!result.is_leader) return result;

  const uint64_t target_index = result.index;
  while (last_applied_.load(std::memory_order_acquire) < target_index) {
    if (is_dead()) break;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return result;
}
bool Raft::has_read_lease() const {
  if (role.load(std::memory_order_acquire) != Role::Leader) return false;
  // Only serve lease reads after we've committed in the current term;
  // this ensures commit_index_ is accurate (not stale from follower phase).
  if (!has_committed_current_term_.load(std::memory_order_acquire)) return false;
  auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
  auto last_ns = last_majority_contact_ns_.load(std::memory_order_acquire);
  return (now_ns - last_ns) < LEASE_DURATION_NS;
}

uint64_t Raft::get_commit_index() const {
  return commit_index_.load(std::memory_order_acquire);
}

uint64_t Raft::get_last_applied() const {
  return last_applied_.load(std::memory_order_acquire);
}

// TODO: add more functions if desired.
void Raft::ticker() {
  while (ticker_running_ && !is_dead()) {
    const int sleep_ms = (role.load(std::memory_order_acquire) == Role::Leader) ? 1 : 10;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    std::lock_guard<std::mutex> lock(mtx);
    if (role == Role::Leader) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - last_heartbeat_time).count();
      const bool due = (elapsed >= HEARTBEAT_INTERVAL_MS);
      const bool pending = has_pending_proposals_;
      if ((due || pending) && !repl_in_flight_) {
        has_pending_proposals_ = false;
        send_heartbeats();
      }
      if (due) last_heartbeat_time = now;
    } else {
      if (election_timeout_elapsed()) {
        start_election();
      }
    }
  }
}

void Raft::reset_election_timer() {
  last_heartbeat_time = std::chrono::steady_clock::now();
  election_timeout = std::chrono::milliseconds(get_random_election_timeout());
}

bool Raft::election_timeout_elapsed() const {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      now - last_heartbeat_time).count();
  return elapsed >= election_timeout.count();
}

int Raft::get_random_election_timeout() const {
  thread_local std::random_device rd;
  thread_local std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(MIN_ELECTION_TIMEOUT_MS,
                                      MAX_ELECTION_TIMEOUT_MS);
  return dis(gen);
}

void Raft::send_heartbeats() {
  if (role != Role::Leader) return;

  std::vector<uint64_t> peer_ids;
  for (const auto &peer : peers_) {
    peer_ids.push_back(peer.first);
  }

  const size_t n_peers = peer_ids.size();
  if (n_peers == 0) return;  // single-node cluster: nothing to replicate

  // Lease counters (per-round shared state)
  const size_t total_nodes_lease = n_peers + 1;
  const size_t majority_lease = total_nodes_lease / 2 + 1;
  auto ack_count = std::make_shared<std::atomic<size_t>>(1);  // leader counts itself
  auto threshold_reached = std::make_shared<std::atomic<bool>>(false);

  // Batching v2: mark this round in-flight and snapshot log horizon + term.
  repl_in_flight_ = true;
  last_flush_time_ns_ = std::chrono::steady_clock::now().time_since_epoch().count();
  const uint64_t round_log_tail = log_.size();
  const uint64_t round_term = current_term;
  auto peers_done = std::make_shared<std::atomic<size_t>>(0);

  for (uint64_t peer_id : peer_ids) {
    std::thread([this, peer_id, n_peers, ack_count, threshold_reached, majority_lease,
                 round_log_tail, round_term, peers_done]() {
      std::unique_lock<std::mutex> lock(mtx);

      // finish() — called at every exit while mtx is held.
      // Decrements the per-round completion counter; the last thread to finish
      // clears repl_in_flight_ and chains a new round if new entries exist.
      auto finish = [&]() {
        if (peers_done->fetch_add(1, std::memory_order_acq_rel) + 1 == n_peers) {
          if (current_term != round_term || role != Role::Leader) {
            // Stale round (role change / re-election). become_follower/become_leader
            // already reset repl_in_flight_; don't touch it here.
            return;
          }
          repl_in_flight_ = false;
          if (log_.size() > round_log_tail) {
            send_heartbeats();  // chain: new entries arrived during this round
          }
        }
      };

      // --- Exit 1: no longer leader before RPC ---
      if (role != Role::Leader) {
        finish();
        return;
      }

      // --- Exit 2: peer not yet initialized ---
      if (next_index_.find(peer_id) == next_index_.end()) {
        finish();
        return;
      }

      uint64_t next_idx = next_index_[peer_id];
      if (next_idx < 1) {
        next_idx = 1;
        next_index_[peer_id] = 1;
      }
      uint64_t prev_log_index = next_idx - 1;
      uint64_t prev_log_term = log_[prev_log_index].term();

      raftpb::AppendEntriesRequest request;
      request.set_term(current_term);
      request.set_leaderid(id);
      request.set_prevlogindex(prev_log_index);
      request.set_prevlogterm(prev_log_term);
      request.set_leadercommit(commit_index_);

      for (uint64_t i = next_idx; i < log_.size(); i++) {
        auto *entry = request.add_entries();
        entry->CopyFrom(log_[i]);
      }

      const uint64_t entries_sent = (log_.size() > next_idx) ? (log_.size() - next_idx) : 0;
      const uint64_t term_at_send = current_term;

      lock.unlock();

      // --- RPC (lock not held) ---
      raftpb::AppendEntriesReply reply;
      grpc::Status status;
      try {
        auto context = this->create_context(peer_id);
        status = this->peers_[peer_id]->AppendEntries(&*context, request, &reply);
      } catch (...) {
        // --- Exit 3 (exception path): re-acquire for finish() ---
        lock.lock();
        finish();
        return;
      }

      // --- Exit 3: RPC failed ---
      if (!status.ok()) {
        lock.lock();
        finish();
        return;
      }

      lock.lock();

      // --- Exit 4: role/term changed while RPC was in flight ---
      if (role != Role::Leader || current_term != term_at_send) {
        finish();
        return;
      }

      // --- Exit 5: peer has higher term → step down ---
      if (reply.term() > current_term) {
        become_follower(reply.term());
        finish();
        return;
      }

      // --- Exit 6: follower rejected (log mismatch) → back off ---
      if (!reply.success()) {
        auto &ni = next_index_[peer_id];
        ni = (ni > 1) ? (ni - 1) : 1;
        finish();
        return;
      }

      // --- Success path: advance follower progress ---
      const uint64_t match = prev_log_index + entries_sent;
      match_index_[peer_id] = std::max<uint64_t>(match_index_[peer_id], match);
      next_index_[peer_id] = match_index_[peer_id] + 1;

      // Lease: update majority contact timestamp
      size_t acks = ack_count->fetch_add(1, std::memory_order_acq_rel) + 1;
      if (acks >= majority_lease &&
          !threshold_reached->exchange(true, std::memory_order_acq_rel)) {
        last_majority_contact_ns_.store(
            std::chrono::steady_clock::now().time_since_epoch().count(),
            std::memory_order_release);
      }

      // Advance commit index if a majority replicated an entry in current term.
      const size_t total_nodes = n_peers + 1;
      const size_t majority = total_nodes / 2 + 1;
      uint64_t new_commit = commit_index_;
      for (uint64_t n = static_cast<uint64_t>(log_.size() - 1); n > commit_index_; n--) {
        if (log_[n].term() != current_term) continue;
        size_t replicated = 1;  // leader itself
        for (const auto &p : match_index_) {
          if (p.second >= n) replicated++;
        }
        if (replicated >= majority) {
          new_commit = n;
          break;
        }
      }

      if (new_commit > commit_index_) {
        commit_index_ = new_commit;
        has_committed_current_term_.store(true, std::memory_order_release);
        applier_cv_.notify_one();
      }

      // --- Exit 7 (normal): finish() before releasing lock ---
      finish();
      lock.unlock();
    }).detach();
  }
}

void Raft::applier_loop() {
  while (applier_running_.load() && !is_dead()) {
    std::vector<ApplyResult> batch;
    {
      std::unique_lock<std::mutex> lock(mtx);
      applier_cv_.wait(lock, [&]() {
        return is_dead() || !applier_running_.load() ||
               (last_applied_ < commit_index_);
      });

      if (is_dead() || !applier_running_.load()) {
        return;
      }

      // Drain all committed entries in one lock acquisition.
      while (last_applied_ < commit_index_) {
        last_applied_++;
        if (last_applied_ >= log_.size()) {
          // Defensive clamp: should not happen in correct Raft.
          last_applied_ = log_.empty() ? 0 : static_cast<uint64_t>(log_.size() - 1);
          break;
        }
        ApplyResult ar;
        ar.valid = true;
        ar.index = last_applied_;
        ar.data = log_[last_applied_].command();
        batch.push_back(ar);
      }
    }

    // Enqueue without holding raft state lock.
    for (auto &ar : batch) {
      apply(ar);
    }
  }
}

void Raft::become_follower(uint64_t term) {
  logger->info("Node {} → FOLLOWER term={}", id, term);
  current_term = term;
  role = Role::Follower;
  voted_for = std::nullopt;
  last_majority_contact_ns_.store(0, std::memory_order_release);  // revoke lease
  has_committed_current_term_.store(false, std::memory_order_release);
  repl_in_flight_ = false;  // stale peer threads will guard via term check in finish()
  has_pending_proposals_ = false;
  last_flush_time_ns_ = 0;
  reset_election_timer();
}

void Raft::become_candidate() {
  current_term++;
  role = Role::Candidate;
  voted_for = id;
  votes_received_.clear();
  votes_received_.insert(id);
  reset_election_timer();
  logger->info("Node {} → CANDIDATE term={}", id, current_term);
}

void Raft::become_leader() {
  logger->info("Node {} → LEADER term={}", id, current_term);
  role = Role::Leader;
  has_committed_current_term_.store(false, std::memory_order_release);
  repl_in_flight_ = false;  // clean state before initial round
  has_pending_proposals_ = false;
  last_flush_time_ns_ = 0;
  for (const auto &peer : peers_) {
    next_index_[peer.first] = log_.size();
    match_index_[peer.first] = 0;
  }
  send_heartbeats();
  last_heartbeat_time = std::chrono::steady_clock::now();
}

void Raft::start_election() {
  logger->info("Node {} timeout → starting election", id);
  become_candidate();
  uint64_t election_term = current_term;
  uint64_t last_log_index = log_.size() - 1;
  uint64_t last_log_term = log_.back().term();

  for (const auto &peer : peers_) {
    std::thread([this, peer_id = peer.first, election_term,
                 last_log_index, last_log_term]() {
      request_vote_from_peer(peer_id, election_term);
    }).detach();
  }
}

void Raft::request_vote_from_peer(uint64_t peer_id, uint64_t election_term) {
    raftpb::RequestVoteRequest request;
  {
    std::lock_guard<std::mutex> lock(mtx);
    // If we are no longer candidate or term changed, abort
    if (role != Role::Candidate || current_term != election_term) {
      return;
    }
    request.set_term(election_term);
    request.set_candidateid(id);
    request.set_lastlogindex(log_.size() - 1);
    request.set_lastlogterm(log_.back().term());
  }

  raftpb::RequestVoteReply reply;
  auto context = create_context(peer_id);
  grpc::Status status = peers_[peer_id]->RequestVote(
      &*context, request, &reply);

  if (!status.ok()) return;

  std::lock_guard<std::mutex> lock(mtx);

  // If we are no longer candidate or term changed, ignore
  if (role != Role::Candidate || current_term != election_term) {
    return;
  }

  // Step down if we see higher term
  if (reply.term() > current_term) {
    become_follower(reply.term());
    return;
  }

  if (reply.votegranted()) {
    votes_received_.insert(peer_id);
    logger->info("Node {} got vote from {} (total={}) in term {}",
                 id, peer_id, votes_received_.size(), current_term);

    // Check majority: need (total_nodes / 2) + 1
    size_t total_nodes = peers_.size() + 1;
    size_t majority = total_nodes / 2 + 1;

    if (votes_received_.size() >= majority) {
      become_leader();
    }
  }
}

bool Raft::is_log_up_to_date(uint64_t last_log_term,
                              uint64_t last_log_index) const {
  uint64_t our_last_term = log_.back().term();
  uint64_t our_last_index = log_.size() - 1;
  if (last_log_term != our_last_term)
    return last_log_term > our_last_term;
  return last_log_index >= our_last_index;
}

} // namespace rafty
