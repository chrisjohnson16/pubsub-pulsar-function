#!/bin/bash
# Run Pulsar function locally with Service Account authentication for GCP
# Assumes Pulsar standalone is running (pulsar standalone)

set -e

[ ! -f "target/pubsub-pulsar-function-1.0.0.jar" ] && echo "Error: Run 'mvn package' first" && exit 1
[ ! -f "function-config.yaml" ] && echo "Error: function-config.yaml not found" && exit 1

echo "🚀 Starting Pulsar → Pub/Sub Function (Service Account)"
echo "   Press Ctrl+C to stop"
echo ""

pulsar-admin functions localrun \
    --jar "$(pwd)/target/pubsub-pulsar-function-1.0.0.jar" \
    --function-config-file "$(pwd)/function-config.yaml"
