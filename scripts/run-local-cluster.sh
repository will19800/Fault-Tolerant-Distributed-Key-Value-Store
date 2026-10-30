#!/usr/bin/env bash
set -euo pipefail

size="${1:-3}"
root="${2:-.local/cluster-${size}}"
if [[ "${size}" != "3" && "${size}" != "5" ]]; then
  echo "usage: $0 [3|5] [runtime-directory]" >&2
  exit 2
fi

mkdir -p "${root}"
binary="${root}/kvserver"
go build -o "${binary}" ./cmd/kvserver

peers=""
for ((index = 1; index <= size; index++)); do
  [[ -n "${peers}" ]] && peers+=","
  peers+="node-${index}=127.0.0.1:$((9000 + index))"
done

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait "${pids[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for ((index = 1; index <= size; index++)); do
  node_dir="${root}/node-${index}"
  mkdir -p "${node_dir}"
  "${binary}" \
    -node-id "node-${index}" \
    -addr "127.0.0.1:$((8000 + index))" \
    -peer-addr "127.0.0.1:$((9000 + index))" \
    -peers "${peers}" \
    -data-dir "${node_dir}" \
    >"${node_dir}/server.log" 2>&1 &
  pids+=("$!")
done

echo "Started ${size}-node cluster. Client URLs:"
for ((index = 1; index <= size; index++)); do
  echo "  node-${index}: http://127.0.0.1:$((8000 + index))"
done
echo "Runtime data and logs: ${root}"
echo "Press Ctrl-C to stop the cluster."
wait
