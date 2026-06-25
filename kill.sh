#!/usr/bin/env bash
set -e

cd /root/task || true

echo "Stopping containers..."
docker compose -f /root/task/docker-compose.yml down --volumes --remove-orphans || true
docker ps -q | xargs -r docker stop || true
docker ps -aq | xargs -r docker rm -f || true

echo "Removing volumes..."
docker volume prune -f || true

echo "Removing networks..."
docker network prune -f || true

echo "Removing images..."
docker rmi -f $(docker images -q | grep -E 'rabbitmq|nodejs|task' || true) || true
docker image prune -a -f || true

echo "Pruning remaining Docker resources..."
docker system prune -a --volumes -f || true

echo "Deleting folder..."
rm -rf /root/task || true

echo "Cleanup completed successfully! Droplet is now clean."
