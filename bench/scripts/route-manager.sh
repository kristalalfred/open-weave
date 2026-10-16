#!/usr/bin/env bash
set -uo pipefail

interval="${ROUTE_INTERVAL:-3}"
sock="${DOCKER_SOCK:-/var/run/docker.sock}"

core_containers="ow-controller ow-controller-2 ow-southbound ow-tls ow-browser ow-browser-2 ow-open-live-strom ow-producer ow-consumer ow-consumer-2"
core_routes="10.97.26.0/24=10.97.25.11 10.97.27.0/24=10.97.25.12"

node1_containers="ow-strom-1 ow-adapter-1"
node1_routes="10.97.25.0/24=10.97.26.2 10.97.27.0/24=10.97.26.2"

node2_containers="ow-strom-2 ow-adapter-2"
node2_routes="10.97.25.0/24=10.97.27.2 10.97.26.0/24=10.97.27.2"

node3_containers="ow-strom-3 ow-adapter-3 ow-producer-3 ow-consumer-3"
node3_routes="10.97.25.0/24=10.97.29.2 10.97.26.0/24=10.97.29.2 10.97.27.0/24=10.97.29.2"

node4_containers="ow-strom-4 ow-adapter-4 ow-producer-4 ow-consumer-4"
node4_routes="10.97.25.0/24=10.97.30.2 10.97.26.0/24=10.97.30.2 10.97.27.0/24=10.97.30.2"

container_pid() {
  curl -s --unix-socket "$sock" "http://localhost/containers/$1/json" 2>/dev/null |
    jq -r 'if .State.Running then .State.Pid else empty end' 2>/dev/null
}

assert_routes() {
  local name="$1" pid cidr via
  shift
  pid="$(container_pid "$name")"
  [ -n "$pid" ] && [ "$pid" != 0 ] || return 0

  for spec in "$@"; do
    cidr="${spec%%=*}"
    via="${spec#*=}"
    if nsenter -t "$pid" -n ip route show "$cidr" 2>/dev/null | grep -q "via $via"; then
      continue
    fi
    if nsenter -t "$pid" -n ip route replace "$cidr" via "$via" 2>/dev/null; then
      echo "$name: installed $cidr via $via"
    else
      echo "$name: failed to install $cidr via $via" >&2
    fi
  done
}

echo "route-manager: asserting bench routes every ${interval}s"
while :; do
  for c in $core_containers; do assert_routes "$c" $core_routes; done
  for c in $node1_containers; do assert_routes "$c" $node1_routes; done
  for c in $node2_containers; do assert_routes "$c" $node2_routes; done
  for c in $node3_containers; do assert_routes "$c" $node3_routes; done
  for c in $node4_containers; do assert_routes "$c" $node4_routes; done
  while read -r name site; do
    case "$site" in
      core) assert_routes "$name" $core_routes ;;
      node1) assert_routes "$name" $node1_routes ;;
      node2) assert_routes "$name" $node2_routes ;;
      node3) assert_routes "$name" $node3_routes ;;
      node4) assert_routes "$name" $node4_routes ;;
    esac
  done < <(curl -s --unix-socket "$sock" --get \
    --data-urlencode 'filters={"label":["open-weave.bench.site","open-weave.bench.project=ow-bench"]}' \
    http://localhost/containers/json | \
    jq -r '.[] | [.Id, .Labels["open-weave.bench.site"]] | @tsv')
  sleep "$interval"
done
