#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "bench_common.hpp"

static std::unique_ptr<BenchCluster> g_cluster;

void handle_sigint(int) {
  g_cluster.reset();
  std::exit(1);
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::fprintf(stderr, "Usage: %s <MaxClientCount> <PutRatio>\n", argv[0]);
    return 1;
  }

  int max_clients = std::atoi(argv[1]);
  int put_ratio = std::atoi(argv[2]); // 0-100

  struct sigaction sa {};
  sa.sa_handler = handle_sigint;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, nullptr);

  // TODO: result.txt column order/header may not match lab spec exactly.
  // Spec wants: clientCount, latAvg, latP50, latP90, latP99, throughput with two-line header.
  // Current: nClients, throughput, latAvg, latP50, latP90, latP99 single-line header.
  // Fix after big benchmark runs are complete.
  std::ofstream result_file("result.txt", std::ios::trunc);
  result_file << "# nClients    throughput(ops/sec)    latAvg(ms)    "
                 "latP50(ms)    latP90(ms)    latP99(ms)\n";
  result_file.close();

  constexpr int OPS_PER_CLIENT = 1000;
  constexpr int NUM_KEYS = 1000;

  for (int num_clients = 1; num_clients <= max_clients; num_clients *= 2) {
    // Fresh cluster per round
    g_cluster = std::make_unique<BenchCluster>();

    // Pre-populate keys using a single client
    {
      kv::KvClient setup_client(g_cluster->kv_addrs());
      for (int k = 1; k <= NUM_KEYS; ++k) {
        setup_client.put("key_" + std::to_string(k),
                         "init_" + std::to_string(k));
      }
    }

    // Per-thread latency collection
    std::vector<std::vector<double>> all_latencies(num_clients);
    std::vector<std::thread> threads;

    auto wall_start = std::chrono::steady_clock::now();

    for (int t = 0; t < num_clients; ++t) {
      threads.emplace_back(
          [&, t]() {
            // Each thread gets its own client (KvClient is not thread-safe)
            kv::KvClient client(g_cluster->kv_addrs());

            // Distinct seed per thread
            std::mt19937 rng(static_cast<unsigned>(t * 1000 + 42));
            std::uniform_int_distribution<int> key_dist(1, NUM_KEYS);
            std::uniform_int_distribution<int> op_dist(0, 99);

            all_latencies[t].reserve(OPS_PER_CLIENT);

            for (int i = 0; i < OPS_PER_CLIENT; ++i) {
              std::string key = "key_" + std::to_string(key_dist(rng));
              bool is_put = (op_dist(rng) < put_ratio);

              auto start = std::chrono::steady_clock::now();
              if (is_put) {
                client.put(key, "val_" + std::to_string(i));
              } else {
                client.get(key);
              }
              auto end = std::chrono::steady_clock::now();

              double ms =
                  std::chrono::duration<double, std::milli>(end - start).count();
              all_latencies[t].push_back(ms);
            }
          });
    }

    for (auto &th : threads) {
      th.join();
    }

    auto wall_end = std::chrono::steady_clock::now();
    double wall_sec =
        std::chrono::duration<double>(wall_end - wall_start).count();

    // Merge all latencies
    std::vector<double> merged;
    for (auto &v : all_latencies) {
      merged.insert(merged.end(), v.begin(), v.end());
    }

    size_t total_ops = merged.size();
    double throughput = total_ops / wall_sec;

    std::sort(merged.begin(), merged.end());
    size_t n = merged.size();
    double avg = std::accumulate(merged.begin(), merged.end(), 0.0) / n;
    double p50 = merged[static_cast<size_t>(0.5 * n)];
    double p90 = merged[static_cast<size_t>(0.9 * n)];
    double p99 = merged[static_cast<size_t>(0.99 * n)];

    std::printf("[%d clients] tput=%.1f ops/s  avg=%.2f ms  p50=%.2f ms  "
                "p90=%.2f ms  p99=%.2f ms\n",
                num_clients, throughput, avg, p50, p90, p99);

    // Append to result.txt
    result_file.open("result.txt", std::ios::app);
    result_file << std::format("{:>10}    {:>18.2f}    {:>10.2f}    {:>10.2f}    "
                               "{:>10.2f}    {:>10.2f}\n",
                               num_clients, throughput, avg, p50, p90, p99);
    result_file.close();

    // Tear down cluster before next round
    g_cluster.reset();

    // Brief pause between rounds for port cleanup
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }

  return 0;
}
