#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-core}"

if [ ! -f ".env" ]; then
  echo "[bootstrap] .env not found, creating from .env.example"
  cp .env.example .env
fi

case "${PROFILE}" in
  core)
    echo "[bootstrap] starting core services..."
    docker compose up -d --build
    ;;
  obs)
    echo "[bootstrap] starting core + observability..."
    docker compose --profile observability up -d --build
    ;;
  all)
    echo "[bootstrap] starting core + observability + ops + ai..."
    docker compose --profile observability --profile ops --profile ai up -d --build
    ;;
  *)
    echo "Usage: $0 [core|obs|all]"
    exit 1
    ;;
esac

echo ""
echo "[bootstrap] done."
echo "API docs: http://localhost:8000/docs"
echo "NATS monitor: http://localhost:8222"
echo "If enabled: Prometheus http://localhost:9090 | Grafana http://localhost:3000"
