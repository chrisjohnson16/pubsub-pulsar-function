# Workload Identity Federation Setup for GCP Pub/Sub

This guide explains how to set up **Workload Identity Federation** to authenticate to Google Cloud Pub/Sub using tokens from external OAuth/OIDC providers like **Okta, Azure AD, Auth0, GitHub Actions**, etc.

## When to Use Workload Identity Federation

Use Workload Identity Federation when:
- You want to authenticate to GCP **without service account keys**
- You have an external identity provider (Okta, Azure AD, Auth0, etc.)
- You're running in a non-GCP environment but want to use GCP services
- You want to follow Google's recommended keyless authentication approach

## How It Works

```
External Provider (Okta/Azure)  →  Workload Identity Pool  →  GCP Service Account  →  Pub/Sub
     (Issues JWT Token)              (Validates & Maps)        (Impersonates)         (Access)
```

1. Your external provider issues a JWT/OIDC token
2. GCP's Workload Identity Pool validates the token
3. GCP impersonates a service account you specify
4. The function uses that service account to publish to Pub/Sub

## Prerequisites

- A GCP project with Pub/Sub API enabled
- An external OAuth/OIDC provider (Okta, Azure AD, etc.)
- `gcloud` CLI installed and authenticated

---

## Step 1: Create a Workload Identity Pool

```bash
# Set your project
export PROJECT_ID="your-project-id"
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# Create the pool
gcloud iam workload-identity-pools create my-pool \
    --project="$PROJECT_ID" \
    --location="global" \
    --display-name="My Workload Identity Pool"
```

---

## Step 2: Create a Workload Identity Provider

The provider configuration depends on your external OAuth provider.

### Option A: OIDC Provider (Okta, Auth0, Azure AD)

```bash
gcloud iam workload-identity-pools providers create-oidc my-oidc-provider \
    --project="$PROJECT_ID" \
    --location="global" \
    --workload-identity-pool="my-pool" \
    --display-name="My OIDC Provider" \
    --attribute-mapping="google.subject=assertion.sub" \
    --issuer-uri="https://YOUR_OIDC_PROVIDER_URL"
```

**Examples:**
- **Okta**: `https://dev-12345.okta.com/oauth2/default`
- **Auth0**: `https://your-tenant.auth0.com/`
- **Azure AD**: `https://login.microsoftonline.com/YOUR_TENANT_ID/v2.0`

### Option B: AWS Provider

```bash
gcloud iam workload-identity-pools providers create-aws my-aws-provider \
    --project="$PROJECT_ID" \
    --location="global" \
    --workload-identity-pool="my-pool" \
    --display-name="My AWS Provider" \
    --attribute-mapping="google.subject=assertion.arn" \
    --account-id="YOUR_AWS_ACCOUNT_ID"
```

### Option C: Generic SAML Provider

```bash
gcloud iam workload-identity-pools providers create-saml my-saml-provider \
    --project="$PROJECT_ID" \
    --location="global" \
    --workload-identity-pool="my-pool" \
    --display-name="My SAML Provider" \
    --attribute-mapping="google.subject=assertion.subject" \
    --idp-metadata-path="saml-metadata.xml"
```

---

## Step 3: Create a GCP Service Account

```bash
# Create service account
gcloud iam service-accounts create pubsub-workload-identity \
    --project="$PROJECT_ID" \
    --display-name="Pub/Sub Workload Identity"

# Grant Pub/Sub Publisher role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:pubsub-workload-identity@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"
```

---

## Step 4: Grant Impersonation Permissions

Allow the external identity to impersonate the GCP service account.

```bash
# For specific external subject (recommended)
gcloud iam service-accounts add-iam-policy-binding \
    pubsub-workload-identity@$PROJECT_ID.iam.gserviceaccount.com \
    --project="$PROJECT_ID" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/my-pool/subject/YOUR_EXTERNAL_SUBJECT"

# OR for all identities in the pool (less secure)
gcloud iam service-accounts add-iam-policy-binding \
    pubsub-workload-identity@$PROJECT_ID.iam.gserviceaccount.com \
    --project="$PROJECT_ID" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/my-pool/*"
```

**Note:** `YOUR_EXTERNAL_SUBJECT` is typically the `sub` claim from your external provider's JWT token.

---

## Step 5: Obtain External Token

How you get the token depends on your provider:

### Okta
```bash
# Use Okta SDK or API to obtain a token
curl -X POST https://dev-12345.okta.com/oauth2/default/v1/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET" \
  -d "scope=openid"
```

### Azure AD
```bash
curl -X POST https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/v2.0/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET" \
  -d "scope=https://graph.microsoft.com/.default"
```

### Auth0
```bash
curl -X POST https://your-tenant.auth0.com/oauth/token \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "audience": "YOUR_API_IDENTIFIER",
    "grant_type": "client_credentials"
  }'
```

Save the token to a file:
```bash
echo "YOUR_JWT_TOKEN" > /tmp/external-token.txt
```

---

## Step 6: Configure the Pulsar Function

Update `function-config-workload-identity.yaml`:

```yaml
userConfig:
  gcp.project.id: "your-project-id"
  gcp.pubsub.topic: "your-topic"
  gcp.pubsub.endpoint: "pubsub.googleapis.com:443"
  
  # Full pool ID (get from step 1)
  gcp.workload.identity.pool.id: "projects/123456789/locations/global/workloadIdentityPools/my-pool"
  
  # Provider name (from step 2)
  gcp.workload.identity.provider.id: "my-oidc-provider"
  
  # Service account email (from step 3)
  gcp.service.account.email: "pubsub-workload-identity@your-project-id.iam.gserviceaccount.com"
  
  # Token file path (from step 5)
  external.subject.token.file: "/tmp/external-token.txt"
  
  # Token type (usually JWT for OIDC)
  external.subject.token.type: "urn:ietf:params:oauth:token-type:jwt"
```

---

## Step 7: Test the Function

```bash
# Build the JAR
mvn clean package

# Run locally
./localrun-workload-identity.sh

# In another terminal, send a test message
pulsar-client produce persistent://public/default/input-topic \
    --messages "Hello from Workload Identity!" \
    --num-produce 1
```

---

## Getting Pool and Provider IDs

### Get Pool ID
```bash
gcloud iam workload-identity-pools describe my-pool \
    --project="$PROJECT_ID" \
    --location="global" \
    --format="value(name)"
```

Output: `projects/123456789/locations/global/workloadIdentityPools/my-pool`

### Get Provider ID
```bash
gcloud iam workload-identity-pools providers describe my-oidc-provider \
    --project="$PROJECT_ID" \
    --location="global" \
    --workload-identity-pool="my-pool" \
    --format="value(name)"
```

Output: `my-oidc-provider` (just the provider name, not the full path)

---

## Token Refresh Considerations

**Important:** External tokens often have short lifetimes (1 hour or less). You'll need to implement token refresh logic:

1. **Option 1: Token Refresh in External System**
   - Have an external process refresh the token and update `/tmp/external-token.txt`
   - Restart the Pulsar function to pick up the new token

2. **Option 2: Dynamic Token Retrieval** (requires code modification)
   - Modify `PubSubPublisherWorkloadIdentityFunction` to periodically fetch fresh tokens from your OAuth provider
   - Use a token cache with expiration

3. **Option 3: Use Credential Configuration File**
   - Store the external provider's credentials in a configuration file
   - Let Google's auth library handle token refresh automatically

---

## Troubleshooting

### Error: "Workload identity pool does not exist"
- Verify your pool ID format: `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_NAME`
- Note: Use **project number**, not project ID

### Error: "Permission denied on service account"
- Check that you granted `roles/iam.workloadIdentityUser` (Step 4)
- Verify the external subject matches your token's `sub` claim

### Error: "Invalid subject token"
- Ensure your external token is a valid JWT
- Check that the `issuer` in your token matches the provider's `issuer-uri`
- Verify token expiration

### Error: "UNAUTHENTICATED"
- Check that your external token file exists and is readable
- Verify the token hasn't expired
- Ensure attribute mapping is correct

---

## Security Best Practices

1. **Limit impersonation scope**: Use specific external subjects, not `/*` wildcards
2. **Rotate tokens regularly**: Set short token lifetimes
3. **Use principle of least privilege**: Grant only `roles/pubsub.publisher`, not broader roles
4. **Audit access**: Enable Cloud Audit Logs for Workload Identity Federation
5. **Secure token storage**: Protect the external token file with appropriate permissions

```bash
chmod 600 /tmp/external-token.txt
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ External OAuth Provider (Okta/Azure/Auth0)                      │
│                                                                   │
│  1. Client requests token                                        │
│  2. Provider issues JWT with claims (sub, iss, exp, etc.)       │
└────────────────────────┬────────────────────────────────────────┘
                         │ JWT Token
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ Pulsar Function (PubSubPublisherWorkloadIdentityFunction)       │
│                                                                   │
│  3. Reads JWT from config/file                                   │
│  4. Creates ExternalAccountCredentials                           │
└────────────────────────┬────────────────────────────────────────┘
                         │ ExternalAccountCredentials
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ GCP Workload Identity Pool                                       │
│                                                                   │
│  5. Validates JWT signature                                      │
│  6. Checks issuer matches provider config                        │
│  7. Maps attributes (sub → google.subject)                       │
└────────────────────────┬────────────────────────────────────────┘
                         │ Validated Identity
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ GCP IAM - Service Account Impersonation                          │
│                                                                   │
│  8. Checks workloadIdentityUser permission                       │
│  9. Impersonates service account                                 │
│ 10. Generates short-lived access token for service account      │
└────────────────────────┬────────────────────────────────────────┘
                         │ Service Account Token
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ Google Cloud Pub/Sub                                             │
│                                                                   │
│ 11. Validates service account has pubsub.publisher role         │
│ 12. Publishes message to topic                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Comparison with Other Auth Methods

| Feature | Service Account Key | OAuth2 (User) | Workload Identity |
|---------|---------------------|---------------|-------------------|
| **Keyless** | ❌ No | ❌ No | ✅ Yes |
| **No Rotation** | ❌ Manual | ⚠️ Automatic | ✅ Automatic |
| **External IdP** | ❌ No | ❌ No | ✅ Yes |
| **Production Ready** | ✅ Yes | ❌ No (user creds) | ✅ Yes |
| **Setup Complexity** | Low | Low | High |
| **Security** | Medium | Low | High |

---

## Additional Resources

- [GCP Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
- [Configuring Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation-with-other-providers)
- [Using Workload Identity Federation](https://cloud.google.com/iam/docs/using-workload-identity-federation)

