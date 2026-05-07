#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
./gradlew clean build "$@"
