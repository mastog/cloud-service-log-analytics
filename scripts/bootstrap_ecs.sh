#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip openjdk-17-jdk wget

if ! command -v hadoop >/dev/null 2>&1; then
  echo "Install Hadoop manually or provide your lab image before running the streaming jobs."
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
python3 -m venv "$PROJECT_ROOT/.venv"
"$PROJECT_ROOT/.venv/bin/pip" install -r "$PROJECT_ROOT/requirements.txt"

echo "Bootstrap finished."
