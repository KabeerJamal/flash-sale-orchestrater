#!/bin/bash

CONTAINERS=(
  "redis"
  "db"
  "reservationpersistenceworker"
  "soldoutworker"
  "outboxworker"
  "paymentworker"
  "rollbackworker"
)

while true; do
  # Wait 10-30 seconds between crashes
  sleep $((RANDOM % 20 + 10))

  # Pick a random container
  TARGET=${CONTAINERS[$RANDOM % ${#CONTAINERS[@]}]}

  echo "Chaos: Killing $TARGET"
  docker stop $TARGET

  # Keep it down for 5-15 seconds
  sleep $((RANDOM % 10 + 5))

  echo "Chaos: Restarting $TARGET"
  docker start $TARGET
done