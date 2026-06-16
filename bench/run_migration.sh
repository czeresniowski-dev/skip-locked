#!/usr/bin/env bash
# Run the Rust normalize hot path and the Python (Celery-era) one over the
# IDENTICAL payloads, under /usr/bin/time so peak RSS is measured uniformly for
# both runtimes. Compute only — no database.
set -euo pipefail
cd "$(dirname "$0")/.."
DIR="$(mktemp -d /tmp/mig.XXXXXX)"
trap 'rm -rf "$DIR"' EXIT

# Build once, then dump payloads + run Rust under the timer.
cargo build --release --example bench_migration >/dev/null 2>&1
BIN="target/release/examples/bench_migration"

echo "== Rust (parse+validate+normalize) =="
/usr/bin/time -l "$BIN" -dump "$DIR" 2>"$DIR/rust.time"
grep -E "maximum resident set size" "$DIR/rust.time" | awk '{printf "  peak RSS: %.1f MB\n", $1/1048576}'

echo "== Python (Celery-era worker compute) =="
/usr/bin/time -l python3 bench/celery_normalize.py "$DIR" 2>"$DIR/py.time"
grep -E "maximum resident set size" "$DIR/py.time" | awk '{printf "  peak RSS: %.1f MB\n", $1/1048576}'

echo
echo "(same payloads, same logic; ratio = python p99 / rust p99. RSS via /usr/bin/time -l,"
echo " bare process — the essay's 280MB was a loaded Django/Celery prefork child.)"
