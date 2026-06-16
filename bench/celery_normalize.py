#!/usr/bin/env python3
"""Reference (interpreted) implementation of the webhook.normalize hot path.

This is the Celery-era worker's compute: parse a carrier tracking webhook,
validate it, normalize it. It reads the EXACT payloads the Rust bench dumped
(bench_migration -dump <dir>), runs the identical work, and prints p50/p99/p99.9
plus the parse+validate share in the same line format as the Rust bench.

Compute only, no database — the database insert was identical before and after
the migration; the parse/validate compute is what changed.
"""
import json
import sys
import time
from datetime import datetime, timezone

STATUSES = {"created", "in_transit", "out_for_delivery", "delivered", "exception", "returned"}
CANONICAL = {
    "created": "CREATED",
    "in_transit": "IN_TRANSIT",
    "out_for_delivery": "OUT_FOR_DELIVERY",
    "delivered": "DELIVERED",
    "exception": "EXCEPTION",
    "returned": "RETURNED",
}


def process(raw, timing):
    t0 = time.perf_counter_ns()
    # parse
    try:
        r = json.loads(raw)
    except ValueError:
        return False
    # validate
    if not r.get("carrier_code") or len(r.get("tracking_number", "")) < 6:
        return False
    ev = r.get("event", {})
    if ev.get("status") not in STATUSES:
        return False
    pkg = r.get("package", {})
    if pkg.get("weight_grams", 0) <= 0 or pkg.get("pieces", 0) <= 0:
        return False
    t1 = time.perf_counter_ns()
    # normalize
    status = CANONICAL.get(ev["status"])
    if status is None:
        return False
    ts = ev["timestamp"].replace("Z", "+00:00")
    ts_millis = int(datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp() * 1000)
    _normalized = {
        "carrier": r["carrier_code"].upper(),
        "tracking": r["tracking_number"],
        "status": status,
        "ts_millis": ts_millis,
        "country": ev["location"]["country"].upper(),
        "billable_grams": max(pkg["weight_grams"], pkg["pieces"] * 500),
    }
    _ = (ev["location"]["city"], ev["location"]["postal"])  # touched fields
    t2 = time.perf_counter_ns()
    timing[0] += t1 - t0
    timing[1] += t2 - t1
    return True


def pct(sorted_lat, p):
    if not sorted_lat:
        return 0.0
    i = int(p / 100 * len(sorted_lat))
    if i >= len(sorted_lat):
        i = len(sorted_lat) - 1
    return sorted_lat[i]


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "/tmp/mig"
    with open(f"{d}/payloads.jsonl") as f:
        payloads = f.read().split("\n")

    lat = []
    timing = [0, 0]  # parse+validate nanos, normalize nanos
    ok = 0
    for raw in payloads:
        if not raw:
            continue
        t0 = time.perf_counter_ns()
        if process(raw, timing):
            ok += 1
        lat.append((time.perf_counter_ns() - t0) / 1000.0)  # microseconds
    lat.sort()
    n = len(lat)
    pv_pct = 100.0 * timing[0] / (timing[0] + timing[1]) if (timing[0] + timing[1]) else 0.0
    print(
        f"PYTHON n={n} ok={ok} p50={pct(lat,50):.2f}us p99={pct(lat,99):.2f}us "
        f"p999={pct(lat,99.9):.2f}us parse_validate_pct={pv_pct:.0f}"
    )


if __name__ == "__main__":
    main()
