#!/usr/bin/env python3
"""
Knee/Saturation Curve Analysis.

Buckets a k6 --out json raw results file's http_req_duration points into the ramp's own stage
windows (matching suite-bsi.js / suite-rdbms.js: 0-15s ramp to 500 TPS, 15-45s ramp to 1000 TPS,
45-75s ramp to 2000 TPS, 75-90s ramp down) and reports observed TPS + latency percentiles per
window, so the knee point (where latency stops being flat as TPS rises) and any saturation plateau
are visible from the actual recorded time series -- not asserted from memory.

Setup() traffic (group "::setup") is excluded: it's charge/VA creation before the ramp starts, not
part of the load test itself.

Usage:
    python3 scenarios/knee-analysis.py --raw scenarios/results/<file>-raw.json.gz
    python3 scenarios/knee-analysis.py --raw scenarios/results/<file>-raw.json  (uncompressed also OK)
"""

import argparse
import gzip
import json
import statistics
import sys
from datetime import datetime

STAGE_BOUNDARIES_SECONDS = [0, 15, 45, 75, 90]
STAGE_LABELS = [
    "Ramp 50->500 TPS",
    "Ramp 500->1000 TPS",
    "Ramp 1000->2000 TPS",
    "Ramp-down 2000->0 TPS",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Knee/saturation analysis from a k6 raw JSON export.")
    parser.add_argument("--raw", required=True, help="Path to the k6 --out json raw results file (.json or .json.gz).")
    return parser.parse_args()


def open_maybe_gzip(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def parse_iso(ts):
    # k6 emits e.g. "2026-07-28T15:22:50.699998+07:00"
    return datetime.fromisoformat(ts)


def load_points(path):
    """Returns a sorted list of (elapsed_seconds, duration_ms) for non-setup http_req_duration points."""
    points = []
    with open_maybe_gzip(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("metric") != "http_req_duration" or rec.get("type") != "Point":
                continue
            data = rec.get("data", {})
            tags = data.get("tags", {})
            if tags.get("group") == "::setup":
                continue
            try:
                t = parse_iso(data["time"])
                v = float(data["value"])
            except (KeyError, ValueError):
                continue
            points.append((t, v))

    if not points:
        print("[ERROR] No non-setup http_req_duration points found in {}".format(path))
        sys.exit(1)

    points.sort(key=lambda p: p[0])
    start = points[0][0]
    return [((t - start).total_seconds(), v) for t, v in points]


def bucket(points):
    buckets = [[] for _ in range(len(STAGE_BOUNDARIES_SECONDS) - 1)]
    for elapsed, v in points:
        for i in range(len(STAGE_BOUNDARIES_SECONDS) - 1):
            lo, hi = STAGE_BOUNDARIES_SECONDS[i], STAGE_BOUNDARIES_SECONDS[i + 1]
            if lo <= elapsed < hi or (i == len(buckets) - 1 and elapsed >= lo):
                buckets[i].append(v)
                break
    return buckets


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def main():
    args = parse_args()
    points = load_points(args.raw)
    buckets = bucket(points)

    print("=" * 100)
    print(" KNEE / SATURATION ANALYSIS: {}".format(args.raw))
    print(" Total non-setup requests: {}".format(len(points)))
    print("=" * 100)
    header = "{:<24} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10}".format(
        "Stage", "Window(s)", "Requests", "Obs.TPS", "p50(ms)", "p95(ms)", "p99(ms)")
    print(header)
    print("-" * len(header))

    for i, values in enumerate(buckets):
        lo, hi = STAGE_BOUNDARIES_SECONDS[i], STAGE_BOUNDARIES_SECONDS[i + 1]
        duration = hi - lo
        count = len(values)
        obs_tps = count / duration if duration else 0
        p50 = pct(values, 50)
        p95 = pct(values, 95)
        p99 = pct(values, 99)
        print("{:<24} {:>10} {:>12} {:>10.1f} {:>10} {:>10} {:>10}".format(
            STAGE_LABELS[i],
            "{}-{}".format(lo, hi),
            count,
            obs_tps,
            "{:.2f}".format(p50) if p50 is not None else "n/a",
            "{:.2f}".format(p95) if p95 is not None else "n/a",
            "{:.2f}".format(p99) if p99 is not None else "n/a",
        ))
    print()


if __name__ == "__main__":
    main()
