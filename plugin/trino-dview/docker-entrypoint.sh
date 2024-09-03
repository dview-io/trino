#!/bin/bash

set -e  # Exit for non-zero
set -x  # Print commands

chown root:root -R /root/ranger-trino-plugin/*

if [ -z "$TRINO_MOUNT_CONFIG_PATH" ]; then
    TRINO_MOUNT_CONFIG_PATH="/mnt/etc/trino"
fi

TRINO_MOUNT_CONFIG_PATH="${TRINO_MOUNT_CONFIG_PATH%/}"
export TRINO_MOUNT_CONFIG_PATH

if [ -z "$TRINO_CONFIG_PATH" ]; then
    TRINO_CONFIG_PATH="/etc/trino"
fi

TRINO_CONFIG_PATH="${TRINO_CONFIG_PATH%/}"
export TRINO_CONFIG_PATH

mkdir -p "$TRINO_CONFIG_PATH"

rm -rf "${TRINO_CONFIG_PATH:?}"/*

cp -a "${TRINO_MOUNT_CONFIG_PATH}/." "$TRINO_CONFIG_PATH"

# Iterate through each file in TRINO_CONFIG_PATH
for file in "${TRINO_CONFIG_PATH%/}/catalog"/*; do
    if [[ -f "$file" ]]; then
        bash /root/replace_env_vars.sh "$file"
    fi
done

if [ "$RANGER_ENABLED" = "true" ]; then
  if [ -z "$RANGER_CONFIG_PATH" ]; then
      RANGER_CONFIG_PATH="/mnt/ranger"
      export RANGER_CONFIG_PATH
  fi
  RANGER_CONFIG_PATH="${RANGER_CONFIG_PATH%/}"
  if [ -f /root/ranger-trino-plugin/install.properties ]; then
      mv -f /root/ranger-trino-plugin/install.properties /root/ranger-trino-plugin/install-backup.properties
  fi

  cp ${RANGER_CONFIG_PATH}/install.properties /root/ranger-trino-plugin/
  bash /root/replace_env_vars.sh "/root/ranger-trino-plugin/install.properties"

  ACCESS_CONTROL_FILE="${TRINO_CONFIG_PATH}/access-control.properties"
  # Check if the file exists
  if [ ! -f "$ACCESS_CONTROL_FILE" ]; then
      touch $ACCESS_CONTROL_FILE
  fi

  /root/ranger-trino-plugin/enable-trino-plugin.sh
fi

/usr/lib/trino/bin/run-trino
