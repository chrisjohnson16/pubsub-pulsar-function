#!/bin/bash

# Local run script for Pub/Sub Publisher with Workload Identity Federation
# Assumes Pulsar standalone is running locally

JAR_FILE="target/pubsub-pulsar-function-1.0.0.jar"
CONFIG_FILE="function-config-workload-identity.yaml"

if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found at $JAR_FILE"
    echo "Run 'mvn clean package' first"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

echo "Starting Pulsar function with Workload Identity Federation..."
pulsar-admin functions localrun \
    --function-config-file "$CONFIG_FILE" \
    --jar "$JAR_FILE"



