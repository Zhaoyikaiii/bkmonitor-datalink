#!/bin/bash
# SurrealDB Query Script
# Usage: ./query.sh "YOUR SQL QUERY"
# Example: ./query.sh "SELECT * FROM pod LIMIT 5"
#
# Note: surreal sql CLI doesn't support skipping TLS verification,
# so we use curl with -k flag for HTTPS connections.

SURREAL_URL="https://127.0.0.1:8000"
SURREAL_USER="root"
SURREAL_PASS="root"
SURREAL_NS="default"
SURREAL_DB="benchmark_v7"

if [ -z "$1" ]; then
    echo "Usage: $0 \"SQL QUERY\""
    echo "Example: $0 \"SELECT * FROM pod LIMIT 5\""
    echo ""
    echo "Interactive mode: $0 -i"
    exit 1
fi

# Interactive mode
if [ "$1" = "-i" ]; then
    echo "SurrealDB Interactive Mode (Ctrl+D to exit)"
    echo "Connected to: ${SURREAL_URL} ns=${SURREAL_NS} db=${SURREAL_DB}"
    echo "---"
    while read -p "> " query; do
        [ -z "$query" ] && continue
        curl -k -s -X POST "${SURREAL_URL}/sql" \
            -H "Accept: application/json" \
            -H "surreal-ns: ${SURREAL_NS}" \
            -H "surreal-db: ${SURREAL_DB}" \
            -u "${SURREAL_USER}:${SURREAL_PASS}" \
            -d "$query" | jq .
    done
    exit 0
fi

curl -k -s -X POST "${SURREAL_URL}/sql" \
    -H "Accept: application/json" \
    -H "surreal-ns: ${SURREAL_NS}" \
    -H "surreal-db: ${SURREAL_DB}" \
    -u "${SURREAL_USER}:${SURREAL_PASS}" \
    -d "$1" | jq .
