# OAuth2 Authentication Setup

Guide for using OAuth2 user credentials instead of service accounts with the Pulsar → Pub/Sub function.

## When to Use OAuth2 vs Service Account

**Use OAuth2 when:**
- Testing locally with your GCP account
- You want user-based authentication
- Building prototypes/demos

**Use Service Account when:**
- Running in production
- Service-to-service authentication
- Automated workloads

## OAuth2 Methods

The function supports two OAuth2 methods:

### Method 1: Access Token (Simplest)

**Pros:**
- Super simple, one command
- Good for quick testing

**Cons:**
- Expires in ~1 hour
- Must refresh manually

### Method 2: Refresh Token (Better)

**Pros:**
- Long-lived (months/years)
- Auto-refreshes access tokens
- Good for extended demos

**Cons:**
- Slightly more setup

---

## Method 1: Access Token

### Step 1: Get Access Token

```bash
# Login (if not already)
gcloud auth application-default login

# Get token
gcloud auth application-default print-access-token
```

Output:
```
ya29.a0AfH6SMBxVeryLongToken...
```

### Step 2: Configure

Edit `function-config-oauth2.yaml`:

```yaml
userConfig:
  gcp.project.id: "your-project-id"
  gcp.pubsub.topic: "your-topic"
  gcp.oauth2.access.token: "ya29.a0AfH6SMBxVeryLongToken..."
```

**Note:** `gcp.oauth2.token.expiry` is optional. If omitted, token works until it expires (~1 hour).

### Step 3: Run

```bash
./localrun-oauth2.sh
```

### Step 4: Refresh When Expired

After ~1 hour, get a new token:
```bash
gcloud auth application-default print-access-token
```

Update config and restart function.

---

## Method 2: Refresh Token

### Step 1: Generate Credentials

```bash
gcloud auth application-default login
```

This creates: `~/.config/gcloud/application_default_credentials.json`

### Step 2: Extract Credentials

```bash
cat ~/.config/gcloud/application_default_credentials.json
```

You'll see:
```json
{
  "client_id": "764086051850-6qr...apps.googleusercontent.com",
  "client_secret": "d-FL95Q19q7...",
  "refresh_token": "1//0gQE8Vxx...",
  "type": "authorized_user"
}
```

### Step 3: Configure

Edit `function-config-oauth2.yaml`:

```yaml
userConfig:
  gcp.project.id: "your-project-id"
  gcp.pubsub.topic: "your-topic"
  
  # Comment out access token if present
  # gcp.oauth2.access.token: "..."
  
  # Add refresh token credentials
  gcp.oauth2.client.id: "764086051850-6qr...apps.googleusercontent.com"
  gcp.oauth2.client.secret: "d-FL95Q19q7..."
  gcp.oauth2.refresh.token: "1//0gQE8Vxx..."
```

### Step 4: Run

```bash
./localrun-oauth2.sh
```

The function will automatically refresh the access token when needed!

---

## Complete Example Config

**Access Token Method:**
```yaml
tenant: "public"
namespace: "default"
name: "pubsub-publisher-oauth2"
className: "com.example.pulsar.function.PubSubPublisherOAuth2Function"

inputs:
  - "persistent://public/default/input-topic"

autoAck: true
parallelism: 1

userConfig:
  gcp.project.id: "my-project-123"
  gcp.pubsub.topic: "my-topic"
  gcp.oauth2.access.token: "ya29.a0AfH6SMBxToken..."

maxMessageRetries: 3
deadLetterTopic: "persistent://public/default/dlq-topic"
```

**Refresh Token Method:**
```yaml
tenant: "public"
namespace: "default"
name: "pubsub-publisher-oauth2"
className: "com.example.pulsar.function.PubSubPublisherOAuth2Function"

inputs:
  - "persistent://public/default/input-topic"

autoAck: true
parallelism: 1

userConfig:
  gcp.project.id: "my-project-123"
  gcp.pubsub.topic: "my-topic"
  gcp.oauth2.client.id: "764086051850-6qr...apps.googleusercontent.com"
  gcp.oauth2.client.secret: "d-FL95Q19q7MQmFpd7hHD0Ty"
  gcp.oauth2.refresh.token: "1//0gQE8VxxxxxxxZ-L9Ir..."

maxMessageRetries: 3
deadLetterTopic: "persistent://public/default/dlq-topic"
```

---

## Troubleshooting

### "OAuth2 credentials not configured"

**Cause:** No access token or complete refresh token credentials provided.

**Fix:** Provide either:
- `gcp.oauth2.access.token`, OR
- All three: `client.id`, `client.secret`, `refresh.token`

### "Invalid credentials" / 401 Unauthorized

**Cause:** Token expired or invalid.

**Fix:** 
- Access token: Get new token with `gcloud auth application-default print-access-token`
- Refresh token: Check credentials match `application_default_credentials.json`

### "Could not load default credentials"

**Cause:** Haven't run `gcloud auth application-default login`

**Fix:**
```bash
gcloud auth application-default login
```

### "Quota exceeded"

**Cause:** OAuth2 user credentials have lower quotas than service accounts.

**Fix:**
- Switch to service account for production
- Request quota increase in GCP Console

### Credentials file location on different OS

- **Linux/Mac:** `~/.config/gcloud/application_default_credentials.json`
- **Windows:** `%APPDATA%\gcloud\application_default_credentials.json`

---

## Security Notes

1. **Never commit credentials** to git
   - Access tokens and refresh tokens are in `.gitignore`
   - Use environment variables or secret management

2. **Rotate regularly**
   - Revoke old refresh tokens in GCP Console
   - Access tokens expire automatically

3. **Use least privilege**
   - Only grant `roles/pubsub.publisher`
   - Don't use Owner or Editor roles

4. **Monitor usage**
   - Check Cloud Audit Logs
   - Set up alerts for unusual activity

---

## Comparison: Access Token vs Refresh Token

| Feature | Access Token | Refresh Token |
|---------|-------------|---------------|
| **Setup** | 1 command | View JSON file |
| **Lifetime** | ~1 hour | Months/years |
| **Auto-refresh** | No | Yes |
| **Best for** | Quick tests | Longer demos |
| **Command** | `print-access-token` | `auth...login` |

---

## When to Switch to Service Account

Switch from OAuth2 to service account when:
- Moving to production
- Running continuously
- Need higher quotas
- Want service-to-service auth

See main [README.md](README.md#service-account-recommended) for service account setup.

---

## Quick Reference

```bash
# Get access token
gcloud auth application-default print-access-token

# Get refresh token credentials
cat ~/.config/gcloud/application_default_credentials.json

# Run function
./localrun-oauth2.sh

# Check Pub/Sub
gcloud pubsub subscriptions pull your-sub --auto-ack --limit=5
```

---

## Resources

- [Google OAuth2 Documentation](https://developers.google.com/identity/protocols/oauth2)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [Service Account Alternative](README.md#service-account-recommended)
