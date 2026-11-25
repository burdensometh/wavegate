#!/bin/sh
set -eu
binary=${1:-./bin/wavegate}
out=${2:-var/demo}
mkdir -p "$(dirname "$out")"
"$binary" demo -fixtures samples/v1/demo-input.json -out "$out"
for evidence in "$out"/*.receipt.v1.json; do
    "$binary" verify-receipt -input "$evidence"
done
for evidence in "$out"/*.window.v1.json; do
    "$binary" verify-window -input "$evidence"
done
