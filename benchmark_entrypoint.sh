#!/usr/bin/env bash

set -e

PROFILES=("postgres" "citus" "scylla" "mongo")
REPEATS=3
OUT_FILE="report/benchmark_results_full.tsv"

cd "$(dirname "$0")"

rm -f "$OUT_FILE"

for db in "${PROFILES[@]}"; do
    echo "======================================"
    echo "Starting profile: $db"

    docker compose --profile "$db" up -d > /dev/null 2>&1

    for q in 1 2 3 4; do
        echo "Running Q$q on $db..."
        python src/scripts/measure_performance.py \
            --db "$db" \
            --query "$q" \
            --runs "$REPEATS" \
            --out "$OUT_FILE" \
            --print-result \
            --save-results-dir "report/query_results/$db"
    done

    echo "Stopping profile: $db"
    docker compose --profile "$db" down > /dev/null 2>&1
    echo "======================================"
done

echo "Benchmarking complete! Results saved to $OUT_FILE"
