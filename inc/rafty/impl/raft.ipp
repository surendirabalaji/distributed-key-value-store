#pragma once

#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "rafty/raft.hpp"

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

inline void Raft::start_server() {
  grpc::EnableDefaultHealthCheckService(false);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  ServerBuilder builder;
  builder.AddListeningPort(this->listening_addr,
                           grpc::InsecureServerCredentials());
  builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1000);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 20000);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

#ifdef TRACING
  builder.experimental().SetInterceptorCreators(
      tracing::CreateServerTracingInterceptors());
#endif

  // TODO: implement RaftService RPC
  // and register the service.
  this->rpc_service_ = std::make_unique<RaftRpcImpl>(*this);
  builder.RegisterService(this->rpc_service_.get());

  std::unique_ptr<Server> server(builder.BuildAndStart());
  logger->info("Raft server {} listening on {}", id, listening_addr);

  this->server_ = std::move(server);

  std::thread([this] { this->server_->Wait(); }).detach();
}

inline void Raft::stop_server() {
  if (this->server_) {
    this->server_->Shutdown();
  }
}

inline void Raft::connect_peers() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 200);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
  args.SetString(GRPC_ARG_OPTIMIZATION_TARGET, "latency");
  args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 20000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

  for (const auto &peer_addr : peer_addrs) {
    logger->info("Connecting to peer {} at {}", peer_addr.first,
                 peer_addr.second);
    std::vector<std::unique_ptr<ClientInterceptorFactoryInterface>>
        interceptor_creators;
    interceptor_creators.push_back(
        std::make_unique<ByteCountingInterceptorFactory>());
    interceptor_creators.push_back(std::make_unique<NetInterceptorFactory>());
#ifdef TRACING
    interceptor_creators.push_back(std::make_unique<tracing::TracingClientInterceptorFactory>());
#endif
    auto channel = CreateCustomChannelWithInterceptors(
        peer_addr.second, grpc::InsecureChannelCredentials(), args,
        std::move(interceptor_creators));
    auto stub = raftpb::RaftService::NewStub(std::move(channel));
    peers_[peer_addr.first] = std::move(stub);
  }
}

inline bool Raft::is_dead() const { return this->dead.load(); }

inline void Raft::kill() {
  this->dead.store(true);
  this->ticker_running_.store(false);
  this->applier_running_.store(false);
  this->applier_cv_.notify_all();
}

inline std::unique_ptr<grpc::ClientContext>
Raft::create_context(uint64_t to) const {
  std::unique_ptr<grpc::ClientContext> context =
      std::make_unique<grpc::ClientContext>();
  context->AddMetadata("from", std::to_string(this->id));
  context->AddMetadata("to", std::to_string(to));
  return context;
}

inline void Raft::apply(const ApplyResult &result) {
  this->ready_queue.enqueue(result);
}

inline grpc::Status Raft::RaftRpcImpl::AppendEntries(
    grpc::ServerContext *context,
    const raftpb::AppendEntriesRequest *request,
    raftpb::AppendEntriesReply *reply) {

  std::unique_lock<std::mutex> lock(raft_.mtx);

  // If we are a leader and another leader claims authority in our term,
  // step down to avoid split-brain behavior.
  if (raft_.role == Role::Leader && request->term() == raft_.current_term &&
      request->leaderid() != raft_.id) {
    raft_.become_follower(request->term());
  }

  // Reject if stale term
  if (request->term() < raft_.current_term) {
    reply->set_term(raft_.current_term);
    reply->set_success(false);
    return grpc::Status::OK;
  }

  // Step down if higher term
  if (request->term() > raft_.current_term) {
    raft_.become_follower(request->term());
  }

  // Reset election timer - valid leader exists
  raft_.reset_election_timer();

  // Candidate steps down
  if (raft_.role == Role::Candidate) {
    raft_.role = Role::Follower;
  }

  reply->set_term(raft_.current_term);

  // Log consistency check
  if (request->prevlogindex() >= raft_.log_.size()) {
    // We don't have the prev entry yet
    reply->set_success(false);
    raft_.logger->info("Node {} rejected AppendEntries: missing prev entry at index {}",
                       raft_.id, request->prevlogindex());
    return grpc::Status::OK;
  }

  if (raft_.log_[request->prevlogindex()].term() != request->prevlogterm()) {
    // Term mismatch at prev index
    reply->set_success(false);
    raft_.logger->info("Node {} rejected AppendEntries: term mismatch at index {} (have {}, need {})",
                       raft_.id, request->prevlogindex(),
                       raft_.log_[request->prevlogindex()].term(),
                       request->prevlogterm());
    return grpc::Status::OK;
  }

  // Consistency check passed - append entries
  uint64_t insert_index = request->prevlogindex() + 1;
  
  for (int i = 0; i < request->entries_size(); i++) {
    const auto& entry = request->entries(i);
    
    if (insert_index < raft_.log_.size()) {
      // Check if existing entry conflicts
      if (raft_.log_[insert_index].term() != entry.term()) {
        // Conflict: delete this and all following entries
        raft_.log_.erase(raft_.log_.begin() + insert_index, raft_.log_.end());
        raft_.log_.push_back(entry);
        raft_.logger->info("Node {} replaced conflicting entry at index {}",
                           raft_.id, insert_index);
      }
      // else: entry already exists and matches, skip
    } else {
      // Append new entry
      raft_.log_.push_back(entry);
      raft_.logger->info("Node {} appended entry at index {} term {}",
                         raft_.id, insert_index, entry.term());
    }
    insert_index++;
  }

  // Part C: advance commit index from leaderCommit and apply newly-committed
  // entries in order (application is handled by a single applier thread).
  const uint64_t last_log_index = raft_.log_.empty() ? 0 : (raft_.log_.size() - 1);
  if (request->leadercommit() > raft_.commit_index_) {
    raft_.commit_index_ = std::min<uint64_t>(request->leadercommit(), last_log_index);
  }

  // If log was truncated, keep indices in range.
  if (raft_.commit_index_ > last_log_index) {
    raft_.commit_index_ = last_log_index;
  }
  if (raft_.last_applied_ > last_log_index) {
    raft_.last_applied_ = last_log_index;
  }

  raft_.applier_cv_.notify_one();

  reply->set_success(true);
  return grpc::Status::OK;
}
inline grpc::Status Raft::RaftRpcImpl::RequestVote(
    grpc::ServerContext *context, const raftpb::RequestVoteRequest *request,
    raftpb::RequestVoteReply *reply) {
  std::lock_guard<std::mutex> lock(raft_.mtx);

  // Reject if candidate's term is stale
  if (request->term() < raft_.current_term) {
    reply->set_term(raft_.current_term);
    reply->set_votegranted(false);
    return grpc::Status::OK;
  }

  // Step down if we see higher term
  if (request->term() > raft_.current_term) {
    raft_.become_follower(request->term());
  }

  reply->set_term(raft_.current_term);

  // Check if we can vote for this candidate:
  // 1. Haven't voted yet OR already voted for this candidate
  // 2. Candidate's log is at least as up-to-date as ours
  bool can_vote = (!raft_.voted_for.has_value() ||
                   raft_.voted_for.value() == request->candidateid());
  bool log_ok = raft_.is_log_up_to_date(request->lastlogterm(),
                                         request->lastlogindex());

  if (can_vote && log_ok) {
    raft_.voted_for = request->candidateid();
    raft_.reset_election_timer();
    reply->set_votegranted(true);
    raft_.logger->info("Node {} voted for {} in term {}",
                       raft_.id, request->candidateid(), raft_.current_term);
  } else {
    reply->set_votegranted(false);
    raft_.logger->info("Node {} denied vote for {} in term {} "
                       "(can_vote={}, log_ok={})",
                       raft_.id, request->candidateid(),
                       raft_.current_term, can_vote, log_ok);
  }
  return grpc::Status::OK;
}

} // namespace rafty
