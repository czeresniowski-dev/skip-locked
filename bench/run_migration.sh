#!/usr/bin/env bash
# Run the Rust normalize hot path and the Python (Celery-era) one over the
# IDENTICAL payloads, with peak RSS measured uniformly for both runtimes.
# Compute only — no database.
set -euo pipefail
cd "$(dirname "$0")/.."
DIR="$(mktemp -d /tmp/mig.XXXXXX)"
trap 'rm -rf "$DIR"' EXIT

# Peak RSS comes from getrusage(RUSAGE_CHILDREN) in a python3 wrapper rather
# than /usr/bin/time: BSD time wants -l where GNU time wants -v (and the GNU
# binary isn't installed everywhere), while python3 is already a dependency
# of this bench. ru_maxrss is bytes on macOS, kilobytes on Linux.
timed() {
  python3 - "$@" <<'PY'
import resource, subprocess, sys
rc = subprocess.call(sys.argv[1:])
peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
peak /= 1 << 20 if sys.platform == "darwin" else 1 << 10
print(f"  peak RSS: {peak:.1f} MB")
sys.exit(rc)
PY
}

# Build once, then dump payloads + run Rust under the timer.
cargo build --release --example bench_migration >/dev/null 2>&1
BIN="target/release/examples/bench_migration"

echo "== Rust (parse+validate+normalize) =="
timed "$BIN" -dump "$DIR"

echo "== Python (Celery-era worker compute) =="
timed python3 bench/celery_normalize.py "$DIR"

echo
echo "(same payloads, same logic; ratio = python p99 / rust p99. RSS via getrusage,"
echo " bare process — the essay's 280MB was a loaded Django/Celery prefork child.)"
