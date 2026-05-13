#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$PROJECT_ROOT"
RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0 "$PYTHON_BIN" -m ray_jobs.degraded_service_detection
