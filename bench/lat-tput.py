#!/usr/bin/env python3
"""Parse a result file and produce a latency-vs-throughput plot.

Usage:
    python3 lat-tput.py <input_file> <output_png> [title]
    python3 lat-tput.py                          # defaults: result.txt -> lat-tput.png
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os
import sys


def main():
    # CLI arguments: input file, output PNG, optional title
    title = "Latency vs Throughput"
    if len(sys.argv) >= 4:
        result_path = sys.argv[1]
        out_path = sys.argv[2]
        title = sys.argv[3]
    elif len(sys.argv) >= 3:
        result_path = sys.argv[1]
        out_path = sys.argv[2]
    elif len(sys.argv) == 2:
        result_path = sys.argv[1]
        out_path = os.path.join(os.path.dirname(__file__), "lat-tput.png")
    else:
        # Legacy fallback: look for result.txt in cwd or parent
        result_path = "result.txt"
        if not os.path.exists(result_path):
            result_path = os.path.join("..", "result.txt")
        out_path = os.path.join(os.path.dirname(__file__), "lat-tput.png")

    if not os.path.exists(result_path):
        print(f"Input file not found: {result_path}", file=sys.stderr)
        sys.exit(1)

    client_counts = []
    throughputs = []
    avg_lats = []
    p50_lats = []
    p90_lats = []
    p99_lats = []

    with open(result_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            try:
                client_counts.append(int(float(parts[0])))
                throughputs.append(float(parts[1]))
                avg_lats.append(float(parts[2]))
                p50_lats.append(float(parts[3]))
                p90_lats.append(float(parts[4]))
                p99_lats.append(float(parts[5]))
            except ValueError:
                continue

    if not throughputs:
        print(f"No data found in {result_path}", file=sys.stderr)
        sys.exit(1)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(throughputs, avg_lats, "o-", label="avg")
    ax.plot(throughputs, p50_lats, "s-", label="p50")
    ax.plot(throughputs, p90_lats, "^-", label="p90")
    ax.plot(throughputs, p99_lats, "D-", label="p99")

    # Annotate each point on the avg series with client count
    for i, nc in enumerate(client_counts):
        ax.annotate(
            str(nc),
            (throughputs[i], avg_lats[i]),
            textcoords="offset points",
            xytext=(6, 4),
            fontsize=8,
            color="gray",
        )

    ax.set_xlabel("Throughput (ops/sec)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved plot to {out_path}")


if __name__ == "__main__":
    main()
