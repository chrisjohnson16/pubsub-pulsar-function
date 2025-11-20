# Pulsar to Google Pub/Sub Function

Demo project showing how to bridge Apache Pulsar and Google Cloud Pub/Sub using Pulsar Functions.

## What It Does

Consumes messages from Pulsar topics and publishes them to Google Cloud Pub/Sub, preserving message properties as attributes.

## Prerequisites

- Java 11+
- Maven 3.6+
- Pulsar standalone (`pulsar standalone`)
- Google Cloud Pub/Sub topic
- GCP credentials (service account **or** OAuth2)

## Quick Start

### 1. Build

```bash
mvn clean package
```

This creates `target/pubsub-pulsar-function-1.0.0.jar` (41MB) - the deployable JAR with all dependencies.

**Note:** Maven also creates `original-*.jar` (8KB, classes only) - you can ignore this.

### 2. Start Pulsar

```bash
pulsar standalone
```

### 3. Configure

**Option A: Service Account** (recommended)

Edit `function-config.yaml`:
```yaml
inputs:
  - "persistent://public/default/input-topic"

userConfig:
  gcp.project.id: "your-project-id"
  gcp.pubsub.topic: "your-topic"
  gcp.credentials.path: "/path/to/service-account-key.json"
```

**Option B: OAuth2** (for testing)

Edit `function-config-oauth2.yaml` - see [OAUTH2-SETUP.md](OAUTH2-SETUP.md)

### 4. Run

```bash
# Service Account
./localrun.sh

# OR OAuth2
./localrun-oauth2.sh
```

### 5. Test

Send a test message:
```bash
pulsar-client produce persistent://public/default/input-topic \
    --messages "Hello Pub/Sub!"
```

Verify in Pub/Sub:
```bash
gcloud pubsub subscriptions pull your-subscription --auto-ack --limit=5
```

## Project Structure

```
├── localrun.sh                     # Run with service account
├── localrun-oauth2.sh              # Run with OAuth2
├── function-config.yaml            # Service account config
├── function-config-oauth2.yaml     # OAuth2 config
├── README.md                       # This file
├── OAUTH2-SETUP.md                 # OAuth2 guide
└── src/main/java/.../
    ├── PubSubPublisherFunction.java        (92 lines)
    └── PubSubPublisherOAuth2Function.java  (123 lines)
```

## Authentication

### Service Account (Recommended)

1. **Create service account key**:
```bash
gcloud iam service-accounts keys create service-account-key.json \
    --iam-account=your-service-account@project.iam.gserviceaccount.com
```

2. **Grant Pub/Sub Publisher role**:
```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT \
    --member="serviceAccount:your-service-account@project.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"
```

3. **Update config** with path to key file

4. **Run**:
```bash
./localrun.sh
```

### OAuth2 (Alternative)

See [OAUTH2-SETUP.md](OAUTH2-SETUP.md) for complete OAuth2 setup.

Quick version:
```bash
# Get access token
gcloud auth application-default print-access-token

# Update function-config-oauth2.yaml with token
# Run
./localrun-oauth2.sh
```

## Configuration Options

### Required
- `gcp.project.id` - Your GCP project ID
- `gcp.pubsub.topic` - Pub/Sub topic name
- `gcp.credentials.path` - Service account key (or OAuth2 creds)

### Optional
- `inputs` - Pulsar topics to consume from (default: input-topic)
- `maxMessageRetries` - Retry failed messages (default: 3)
- `deadLetterTopic` - Failed messages destination
- `autoAck` - Auto-acknowledge processed messages (default: true)
- `parallelism` - Number of function instances (default: 1)

## Message Properties

Pulsar message properties are preserved as Pub/Sub attributes:

```bash
# Send message with properties
pulsar-client produce persistent://public/default/input-topic \
    --messages "Message with metadata" \
    --properties "source=pulsar,priority=high,region=us-east"
```

These become Pub/Sub message attributes.

## Production Deployment

Deploy (not just localrun):

```bash
pulsar-admin functions create \
    --jar target/pubsub-pulsar-function-1.0.0.jar \
    --function-config-file function-config.yaml
```

Manage:
```bash
# Status
pulsar-admin functions status --name pubsub-publisher

# Logs  
pulsar-admin functions logs --name pubsub-publisher

# Stop
pulsar-admin functions stop --name pubsub-publisher

# Delete
pulsar-admin functions delete --name pubsub-publisher
```

## How It Works

Both functions:
1. Initialize Pub/Sub publisher on first message
2. Read message bytes and properties from Pulsar
3. Build Pub/Sub message with properties as attributes
4. Publish to Pub/Sub and log the message ID

**Service Account version** (92 lines): Uses GCP service account key or Application Default Credentials

**OAuth2 version** (123 lines): Uses OAuth2 access tokens or refresh tokens

## Troubleshooting

### "Required config missing: gcp.project.id"
Update your `function-config.yaml` with actual values.

### "Unable to find credentials"
- Service Account: Check `gcp.credentials.path` points to valid key file
- OAuth2: Run `gcloud auth application-default login`

### "Permission denied" on Pub/Sub
Grant the publisher role:
```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:SERVICE_ACCOUNT" \
    --role="roles/pubsub.publisher"
```

### Messages not appearing in Pub/Sub
- Check function logs for errors
- Verify topic exists: `gcloud pubsub topics list`
- Confirm message was sent: `pulsar-admin topics stats persistent://public/default/input-topic`

### Function won't start
- Ensure Pulsar is running: `pulsar-admin brokers list`
- Check JAR exists: `ls -lh target/*.jar`
- Verify config file syntax

## Development

```bash
# Clean build
mvn clean package

# Quick rebuild
mvn package -q

# View logs (when running)
# Ctrl+C to stop
```

## Resources

- [Pulsar Functions Docs](https://pulsar.apache.org/docs/functions-overview/)
- [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs)
- [OAuth2 Setup](OAUTH2-SETUP.md)

## License

Open source - use freely!
