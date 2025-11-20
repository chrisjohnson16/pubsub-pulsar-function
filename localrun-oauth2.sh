#!/bin/bash
# Run Pulsar function locally with OAuth2 authentication for GCP
# Assumes Pulsar standalone is running (pulsar standalone)

set -e

[ ! -f "target/pubsub-pulsar-function-1.0.0.jar" ] && echo "Error: Run 'mvn package' first" && exit 1
[ ! -f "function-config-oauth2.yaml" ] && echo "Error: function-config-oauth2.yaml not found" && exit 1

echo "🚀 Starting Pulsar → Pub/Sub Function (OAuth2)"
echo "   See OAUTH2-SETUP.md for credential setup"
echo "   Press Ctrl+C to stop"
echo ""

pulsar-admin functions localrun \
    --jar "$(pwd)/target/pubsub-pulsar-function-1.0.0.jar" \
    --function-config-file "$(pwd)/function-config-oauth2.yaml"

