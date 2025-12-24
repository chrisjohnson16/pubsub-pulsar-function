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
    --client-auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken \
    --client-auth-params "token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDY3MDg0NjAsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50O2Y2OWU2OWEyLTM5ODEtNDRhMS04MDQ4LTQ1YjU0MmQwOTA2NDtZWE10WkdWdGJ3PT07MmNkMTc4YmQ1MyIsInRva2VuaWQiOiIyY2QxNzhiZDUzIn0.irpvSnOPAwcnsMNWDg8W5hGd4cJawnpHjZ6Xrc7ThlMuCaExXv-2sfXzOufHgiM_B3e_5x1nO8XiaYdfBI5VmInYsORJJL9o92Kwpu01cYSbwDCpvrXiwKb_5JT5zBB81YtVybM1jFooWm-dZuFNgEXnc0llBxfeAeB5DjKFtDZuUdeDml0_2ovm8kTRgEvhG6fhDkroWHTHdrDsePtydqWEsvWJQnB4u8IR1vy_MhhyOQeaiw4pekjFj2QqsjZarlj27OvC8u43ksqgFtu6msl-mxo25pHvTekfD722Am-KDLi6LTdQN6aU06nktI1dM6-H-e83Qzi5rSybkrUymg" \
    --jar "$(pwd)/target/pubsub-pulsar-function-1.0.0.jar" \
    --function-config-file "$(pwd)/function-config.yaml" 
    