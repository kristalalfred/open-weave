#!/usr/bin/env bash
set -euo pipefail

addr="${1:-}"
if [ -z "$addr" ]; then
  echo "usage: inside.sh <host[:port]>" >&2
  exit 2
fi

case "${addr%%:*}" in
  10.97.29.*) echo "-3" ;;
  10.97.30.*) echo "-4" ;;
  *) echo "" ;;
esac
