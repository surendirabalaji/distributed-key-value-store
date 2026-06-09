#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "bench_common.hpp"

static std::unique_ptr<BenchCluster> g_cluster;

void handle_sigint(int) {
  g_cluster.reset();
  std::exit(1);
}

int main() {
  struct sigaction sa {};
  sa.sa_handler = handle_sigint;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, nullptr);

  g_cluster = std::make_unique<BenchCluster>();
  kv::KvClient client(g_cluster->kv_addrs());

  constexpr int TOTAL_OPS = 1000;
  constexpr int WARMUP_OPS = 50;

  int success_count = 0;
  int fail_count = 0;

  std::vector<double> latencies;
  latencies.reserve(TOTAL_OPS - WARMUP_OPS);

  for (int i = 0; i < TOTAL_OPS; ++i) {
    std::string key = "lat_key_" + std::to_string(i);
    std::string val = "lat_val_" + std::to_string(i);

    auto start = std::chrono::steady_clock::now();
    auto put_status = client.put(key, val);
    auto end = std::chrono::steady_clock::now();

    if (put_status == kvpb::KV_SUCCESS) {
      success_count++;
      if (i >= WARMUP_OPS) {
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        latencies.push_back(ms);
      }
    } else {
      fail_count++;
      std::cerr << "[latency] op " << i << " FAILED with status=" << put_status
                << std::endl;
    }
  }

  std::cerr << "[latency] successful: " << success_count
            << ", failed: " << fail_count
            << ", samples: " << latencies.size() << std::endl;

  std::sort(latencies.begin(), latencies.end());

  size_t n = latencies.size();
  double avg =
      std::accumulate(latencies.begin(), latencies.end(), 0.0) / n;
  double p50 = latencies[static_cast<size_t>(0.5 * n)];
  double p99 = latencies[static_cast<size_t>(0.99 * n)];

  std::printf("######################################\n");
  std::printf("#      latAvg       latP50      latP99\n");
  std::printf("#        (ms)         (ms)        (ms)\n");
  std::printf("--------------------------------------\n");
  std::printf("    %9.2f     %9.2f   %9.2f\n", avg, p50, p99);

  g_cluster.reset();
  return 0;
}
