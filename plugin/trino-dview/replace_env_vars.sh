#!/bin/bash

# Function to replace ${env:VARIABLE_NAME:-default_value} in YAML file
replace_env_vars_yaml() {
    local file="$1"
    local tmp_file="${file}.tmp"

    # Read the file line by line
    while IFS= read -r line; do
        # Match ${env:VARIABLE_NAME:-default_value} pattern
        while [[ $line =~ (\$\{env:([^:-]+)(:-([^}]+))?}) ]]; do
            env_var="${BASH_REMATCH[2]}"
            default_value="${BASH_REMATCH[4]}"
            full_match="${BASH_REMATCH[1]}"
            if [[ -n $default_value ]]; then
                value="${!env_var:-$default_value}"
            else
                value="${!env_var:-}"
            fi
            line="${line//$full_match/$value}"
        done
        echo "$line"
    done < "$file" > "$tmp_file"

    # Replace the original file with the modified one
    mv "$tmp_file" "$file"
}

# Function to replace ${env:VARIABLE_NAME:-default_value} in JSON file
replace_env_vars_json() {
    local file="$1"
    local tmp_file="${file}.tmp"

    # Read the file line by line
    while IFS= read -r line; do
        # Match ${env:VARIABLE_NAME:-default_value} pattern
        while [[ $line =~ (\$\{env:([^:-]+)(:-([^}]+))?}) ]]; do
            env_var="${BASH_REMATCH[2]}"
            default_value="${BASH_REMATCH[4]}"
            full_match="${BASH_REMATCH[1]}"
            if [[ -n $default_value ]]; then
                value="${!env_var:-$default_value}"
            else
                value="${!env_var:-}"
            fi
            line="${line//$full_match/$value}"
        done
        echo "$line"
    done < "$file" > "$tmp_file"

    # Replace the original file with the modified one
    mv "$tmp_file" "$file"
}

# Function to replace ${env:VARIABLE_NAME:-default_value} in properties file
replace_env_vars_properties() {
    local file="$1"
    local tmp_file="${file}.tmp"

    # Read the file line by line
    while IFS= read -r line; do
        # Match ${env:VARIABLE_NAME:-default_value} pattern
        while [[ $line =~ (\$\{env:([^:-]+)(:-([^}]+))?}) ]]; do
            env_var="${BASH_REMATCH[2]}"
            default_value="${BASH_REMATCH[4]}"
            full_match="${BASH_REMATCH[1]}"
            if [[ -n $default_value ]]; then
                value="${!env_var:-$default_value}"
            else
                value="${!env_var:-}"
            fi
            line="${line//$full_match/$value}"
        done
        echo "$line"
    done < "$file" > "$tmp_file"

    # Replace the original file with the modified one
    mv "$tmp_file" "$file"
}


replace_env_vars() {
    local file="$1"
    local extension="${file##*.}"  # Extract file extension

    case "$extension" in
        yaml|yml)
            replace_env_vars_yaml "$file"
            ;;
        json)
            replace_env_vars_json "$file"
            ;;
        properties)
            replace_env_vars_properties "$file"
            ;;
        *)
            echo "Error: Unsupported file format. Only YAML (yaml/yml) or JSON files are supported."
            echo "Hence Skipping file $file"
    esac
}

if [ $# -eq 0 ]; then
    echo "Usage replace_env_vars.sh path/to/file"
    exit 1
fi

replace_env_vars "$1"
