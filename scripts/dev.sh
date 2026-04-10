#!/usr/bin/env bash
set -e

cleanup() {
    echo "Shutting down..."
    kill "$SERVER_PID" 2>/dev/null
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "Starting Prefect server..."
uv run prefect server start &
SERVER_PID=$!

echo "Waiting for Prefect server to be ready..."
until curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
    sleep 1
done

echo "Prefect server ready. Starting deploy..."
uv run --env-file .env python deploy.py
