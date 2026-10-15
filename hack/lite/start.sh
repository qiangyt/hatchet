#!/bin/bash

# Run migration script
./hatchet-migrate
if [ $? -ne 0 ]; then
  echo "Migration script failed. Exiting..."
  exit 1
fi

# Generate config files
./hatchet-admin quickstart --skip certs --generated-config-dir ./config --overwrite=false

# In authdisabled builds, mint and surface the default worker token
if ./hatchet-admin authdisabled; then
  if [ ! -s ./config/authdisabled-token ]; then
    ./hatchet-admin token create --config ./config --name authdisabled-default > ./config/authdisabled-token
  fi
  echo "================ authdisabled build: worker API token ==============="
  echo "Set HATCHET_CLIENT_TOKEN to the value below (also at ./config/authdisabled-token):"
  cat ./config/authdisabled-token
  echo
  echo "====================================================================="
fi

# Optional durable file log: when LITE_LOG_FILE is set (typically a bind
# mount), tee stdout+stderr into it so engine logs survive container
# recreation — docker logs are lost with the container, and the 2026-10-15
# partition-outage investigation was blocked exactly by that. Process
# substitution keeps the binary as PID 1 (via exec), so SIGTERM from
# `docker stop` still reaches hatchet-lite for graceful shutdown; stdout keeps
# flowing to docker logs in parallel.
if [ -n "$LITE_LOG_FILE" ]; then
  mkdir -p "$(dirname "$LITE_LOG_FILE")"
  echo "logging to file: $LITE_LOG_FILE"
  exec > >(tee -a "$LITE_LOG_FILE") 2>&1
fi

# Run the Go binary
exec ./hatchet-lite --config ./config
