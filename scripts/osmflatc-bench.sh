#!/usr/bin/env bash
# Iterative osmflatc perf-tuning helper.
#
# Runs the release osmflatc binary with debug-level logging (so the
# [timing] phase="..." secs=... lines added throughout osmflatc/src land in
# the log), samples iostat for the duration of the run, and appends every
# parsed timing line to a running CSV -- one row per phase per run -- so
# successive runs with different tuning flags can be diffed.
#
# Usage:
#   scripts/osmflatc-bench.sh --label <name> -- <osmflatc args...>
#
# Example:
#   scripts/osmflatc-bench.sh --label baseline -- \
#       us-west-latest.osm.pbf us-west.osm.flat \
#       --max-open-files 1000 --write-buffer-mb 4000 --block-cache-mb 4000 --flat-nodes
#
# Compare across runs:
#   awk -F, '$1=="baseline" || $1=="readahead-4mb"' bench-results/timings.csv \
#       | sort -t, -k3,3 -k1,1
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/bench-results"
mkdir -p "$RESULTS_DIR"

LABEL="run"
if [[ "${1:-}" == "--label" ]]; then
    LABEL="$2"
    shift 2
fi
if [[ "${1:-}" == "--" ]]; then
    shift
fi
if [[ $# -eq 0 ]]; then
    echo "usage: $0 --label <name> -- <osmflatc args...>" >&2
    exit 1
fi

TS="$(date +%Y%m%d-%H%M%S)"
LOG="$RESULTS_DIR/${LABEL}-${TS}.log"
IOSTAT_LOG="$RESULTS_DIR/${LABEL}-${TS}.iostat.log"
CSV="$RESULTS_DIR/timings.csv"

BIN="$REPO_ROOT/target/release/osmflatc"
if [[ ! -x "$BIN" ]]; then
    echo "Building release osmflatc..." >&2
    (cd "$REPO_ROOT" && cargo build --release -p osmflatc)
fi

# Ensure at least debug-level logging (-v) so [timing] lines are emitted,
# without clobbering a caller-supplied -vv/-vvv.
ARGS=("$@")
have_v=0
for a in "${ARGS[@]}"; do
    if [[ "$a" == -v* ]]; then
        have_v=1
    fi
done
if [[ "$have_v" -eq 0 ]]; then
    ARGS=(-v "${ARGS[@]}")
fi

# Best-effort background iostat sampling: a missing binary or an
# unsupported invocation must not fail the benchmark run.
IOSTAT_PID=""
if command -v iostat >/dev/null 2>&1; then
    if [[ "$(uname)" == "Darwin" ]]; then
        iostat -w 5 >"$IOSTAT_LOG" 2>&1 &
    else
        iostat -x 5 >"$IOSTAT_LOG" 2>&1 &
    fi
    IOSTAT_PID=$!
fi
cleanup() {
    if [[ -n "$IOSTAT_PID" ]]; then
        kill "$IOSTAT_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "[bench] label=$LABEL bin=$BIN args=${ARGS[*]}" | tee "$LOG"
START=$(date +%s)
set +e
"$BIN" "${ARGS[@]}" 2>&1 | tee -a "$LOG"
STATUS=${PIPESTATUS[0]}
set -e
END=$(date +%s)
echo "[bench] wall_clock_secs=$((END - START)) exit=$STATUS" | tee -a "$LOG"

if [[ ! -f "$CSV" ]]; then
    echo "label,timestamp,phase,secs" >"$CSV"
fi
grep '\[timing\]' "$LOG" \
    | sed -E 's/.*phase="([^"]+)" secs=([0-9.]+).*/\1,\2/' \
    | while IFS=, read -r phase secs; do
        echo "${LABEL},${TS},${phase},${secs}" >>"$CSV"
    done

echo
echo "[bench] top phases by duration (this run):"
grep '\[timing\]' "$LOG" \
    | sed -E 's/.*phase="([^"]+)" secs=([0-9.]+).*/\2 \1/' \
    | sort -rn \
    | head -20

echo
echo "[bench] log:    $LOG"
echo "[bench] iostat: $IOSTAT_LOG"
echo "[bench] csv:    $CSV"

exit "$STATUS"
