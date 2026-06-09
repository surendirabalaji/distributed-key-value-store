#pragma once

#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

#include "kv/kv_client.hpp"

static constexpr uint64_t KV_PORT_OFFSET = 1000;

class BenchCluster {
public:
  explicit BenchCluster(uint64_t num_replicas = 3,
                        uint64_t raft_base_port = 50050,
                        const std::string &kv_node_path = "./app/kv_node")
      : num_replicas_(num_replicas) {
    rafty::utils::init_logger();

    auto logger_name = "bench";
    logger_ = spdlog::get(logger_name);
    if (!logger_) {
      logger_ = spdlog::basic_logger_mt(
          logger_name, std::format("logs/{}.log", logger_name), true);
    }

    auto insts =
        toolings::ConfigGen::gen_local_instances(num_replicas, raft_base_port);

    std::vector<rafty::Config> configs;
    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55001;

    for (const auto &inst : insts) {
      std::map<uint64_t, std::string> peer_addrs;
      for (const auto &peer : insts) {
        if (peer.id == inst.id)
          continue;
        peer_addrs[peer.id] = peer.external_addr;
      }
      rafty::Config config = {
          .id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs};
      configs.push_back(config);
      node_tester_ports[inst.id] = tester_port++;

      kv_addrs_.push_back(
          "localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }

    const std::string ctrl_addr = "0.0.0.0:55000";
    ctrl_ = std::make_unique<toolings::RaftTestCtrl>(
        configs, node_tester_ports, kv_node_path, ctrl_addr, 0, 0, logger_);

    ctrl_->register_applier_handler(
        [](testerpb::ApplyResult) -> void {});

    ctrl_->run();

    // Wait for leader election to stabilize
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  ~BenchCluster() {
    if (ctrl_) {
      ctrl_->kill();
      ctrl_.reset();
    }
  }

  // Non-copyable, non-movable
  BenchCluster(const BenchCluster &) = delete;
  BenchCluster &operator=(const BenchCluster &) = delete;

  const std::vector<std::string> &kv_addrs() const { return kv_addrs_; }

private:
  uint64_t num_replicas_;
  std::shared_ptr<spdlog::logger> logger_;
  std::unique_ptr<toolings::RaftTestCtrl> ctrl_;
  std::vector<std::string> kv_addrs_;
};
