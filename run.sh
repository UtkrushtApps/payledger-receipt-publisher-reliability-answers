#!/usr/bin/env bash
set -e

cd /root/task

echo "==> Installing Node.js dependencies..."
npm install

echo "==> Starting RabbitMQ and application services..."
docker compose up -d --build

echo "==> Waiting for RabbitMQ to become healthy..."
rabbit_ready=""
for i in $(seq 1 30); do
  status=$(docker inspect --format '{{.State.Health.Status}}' "$(docker compose ps -q rabbitmq)" 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    rabbit_ready="yes"
    echo "RabbitMQ is healthy."
    break
  fi
  echo "   ...still waiting on RabbitMQ ($i/30) [$status]"
  sleep 3
done
if [ -z "$rabbit_ready" ]; then
  echo "RabbitMQ did not become healthy in time."
  docker compose logs
  exit 1
fi

echo "==> Waiting for API health endpoint..."
api_ready=""
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:3000/health >/dev/null 2>&1; then
    api_ready="yes"
    echo "API is responding."
    break
  fi
  echo "   ...still waiting on API ($i/30)"
  sleep 2
done
if [ -z "$api_ready" ]; then
  echo "API health endpoint did not respond in time."
  docker compose logs
  exit 1
fi

echo "==> Waiting for worker readiness endpoint..."
worker_ready=""
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:3100/health >/dev/null 2>&1; then
    worker_ready="yes"
    echo "Worker is responding."
    break
  fi
  echo "   ...still waiting on worker ($i/30)"
  sleep 2
done
if [ -z "$worker_ready" ]; then
  echo "Worker readiness endpoint did not respond in time."
  docker compose logs
  exit 1
fi

echo "==> Running starter readiness smoke check..."
node scripts/readiness-check.js

echo "==> SUCCESS: RabbitMQ, API, and worker starter services are up and ready."
