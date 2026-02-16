#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL_PG:?DATABASE_URL_PG is required for pg_dump}"

BACKUP_DIR="${BACKUP_DIR:-/backups}"
INTERVAL_MIN="${BACKUP_INTERVAL_MIN:-1440}"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-7}"

mkdir -p "$BACKUP_DIR"

echo "[backup] DATABASE_URL_PG=${DATABASE_URL_PG}"
echo "[backup] BACKUP_DIR=${BACKUP_DIR}"
echo "[backup] INTERVAL_MIN=${INTERVAL_MIN}"
echo "[backup] KEEP_DAYS=${KEEP_DAYS}"

while true; do
  TS="$(date -u +'%Y%m%d_%H%M%S')"
  OUT="${BACKUP_DIR}/quant_${TS}.sql.gz"
  echo "[backup] dumping -> ${OUT}"
  pg_dump "${DATABASE_URL_PG}" | gzip > "${OUT}"
  echo "[backup] ok"

  # retention
  find "${BACKUP_DIR}" -type f -name "quant_*.sql.gz" -mtime "+${KEEP_DAYS}" -print -delete || true

  echo "[backup] sleep ${INTERVAL_MIN} minutes..."
  sleep "${INTERVAL_MIN}m"
done
