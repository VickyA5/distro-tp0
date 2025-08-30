#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <archivo_salida> <num_clientes>"
    echo "Example: $0 docker-compose-dev.yaml 5"
    exit 1
fi

OUTPUT_FILE=$1
NUM_CLIENTS=$2

echo "Output file name: $OUTPUT_FILE"
echo "Number of clients: $NUM_CLIENTS"

if ! [[ "$NUM_CLIENTS" =~ ^[0-9]+$ ]] || [ "$NUM_CLIENTS" -lt 0 ]; then
    echo "Error: The number of clients must be a non-negative integer"
    exit 1
fi

python3 generador-compose.py "$OUTPUT_FILE" "$NUM_CLIENTS"

if [ $? -eq 0 ]; then
    echo "Docker Compose successfully generated"
else
    echo "Error generating Docker Compose file"
    exit 1
fi