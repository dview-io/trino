#!/bin/bash

chown root:root -R /root/ranger-trino-plugin/*

# Check and set TRINO_MOUNT_CONFIG_PATH
if [ -z "$TRINO_MOUNT_CONFIG_PATH" ]; then
    TRINO_MOUNT_CONFIG_PATH="/mnt/etc/trino"
    export TRINO_MOUNT_CONFIG_PATH
else
    # any trailing slashes
    TRINO_MOUNT_CONFIG_PATH="${TRINO_MOUNT_CONFIG_PATH%/}"
fi

# Check and set TRINO_CONFIG_PATH
if [ -z "$TRINO_CONFIG_PATH" ]; then
    TRINO_CONFIG_PATH="/etc/trino"
    export TRINO_CONFIG_PATH
else
    # any trailing slashes
    TRINO_CONFIG_PATH="${TRINO_CONFIG_PATH%/}"
fi

# Check and set $RANGER_CONFIG_PATH
if [ -z "$RANGER_CONFIG_PATH" ]; then
    $RANGER_CONFIG_PATH="/mnt/ranger"
    export RANGER_CONFIG_PATH
else
    RANGER_CONFIG_PATH="${RANGER_CONFIG_PATH%/}"
fi

TRINO_CONFIG_PATH=${TRINO_CONFIG_PATH%/}

cp -r ${TRINO_MOUNT_CONFIG_PATH} ${TRINO_CONFIG_PATH}


# Iterate through each file in TRINO_MOUNT_CONFIG_PATH
for file in "${TRINO_CONFIG_PATH%/}/trino"/*; do
    if [[ -f "$file" ]]; then
        # Replace environment variables in the file using replace_env_vars.sh
        bash /root/replace_env_vars.sh "$file"
        echo "File '$file' processed and copied to '${TRINO_CONFIG_PATH%/}/trino/catalog/$(basename "$file")'."
    fi
done

cp /mnt/ranger/install.properties /root/ranger-trino-plugin/
bash /root/replace_env_vars.sh "/root/ranger-trino-plugin/install.properties"


ACCESS_CONTROL_FILE="${TRINO_CONFIG_PATH}/trino/access-control.properties"
# Check if the file exists
if [ ! -f "$ACCESS_CONTROL_FILE" ]; then
    touch $ACCESS_CONTROL_FILE
fi

/root/ranger-trino-plugin/enable-trino-plugin.sh

/usr/lib/trino/bin/run-trino
