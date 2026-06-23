#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_dir}/fuzz"

targets=(
  kv_model
  multi_bucket_model
  reopen_compact_model
  bucket_lifecycle
  concurrent_snapshot_model
)

export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"

for target in "${targets[@]}"; do
  cargo +nightly fuzz run "${target}" -- -max_total_time=600
done
