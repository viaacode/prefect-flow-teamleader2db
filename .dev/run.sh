#!/usr/bin/env bash
# Start Postgres and the mock Teamleader API in Docker, run the end-to-end tests
# against them, and tear everything down again. Extra arguments go to pytest.
set -euo pipefail

cd "$(dirname "$0")/.."

compose="docker compose -f .dev/docker-compose.yml"
trap '$compose down -v' EXIT

$compose up -d --build --wait

DEV_STACK=1 .venv/bin/python -m pytest tests/integration "$@"
