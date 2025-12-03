package com.example.pulsar.function;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar Function that publishes messages to Google Cloud Pub/Sub using Workload Identity Federation.
 * This allows authentication using tokens from external OAuth providers (Okta, Azure AD, Auth0, etc.).
 *
 * Required config: gcp.project.id, gcp.pubsub.topic, gcp.pubsub.endpoint
 * Required config: gcp.workload.identity.pool.id, gcp.workload.identity.provider.id, gcp.service.account.email
 * Required config: external.subject.token (or external.subject.token.file)
 *
 * Optional config: external.subject.token.type (defaults to "urn:ietf:params:oauth:token-type:jwt")
 */
public class PubSubPublisherWorkloadIdentityFunction implements Function<byte[], Void> {

    private Publisher publisher;
    private Logger logger;

    @Override
    public Void process(byte[] input, Context context) throws Exception {
        // Lazy initialization on first message
        if (publisher == null) {
            initializePublisher(context);
        }

        // Build Pub/Sub message with Pulsar properties as attributes
        PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(input));

        Map<String, String> properties = context.getCurrentRecord().getProperties();
        if (properties != null && !properties.isEmpty()) {
            messageBuilder.putAllAttributes(properties);
        }

        // Publish synchronously and log message ID
        String messageId = publisher.publish(messageBuilder.build()).get();
        logger.info("Published message to Pub/Sub: {}", messageId);

        return null;
    }

    private void initializePublisher(Context context) throws Exception {
        logger = context.getLogger();
        Map<String, Object> config = context.getUserConfigMap();

        // Read required configuration
        String projectId = getRequiredConfig(config, "gcp.project.id");
        String topicId = getRequiredConfig(config, "gcp.pubsub.topic");
        String endpoint = getRequiredConfig(config, "gcp.pubsub.endpoint");
        
        String poolId = getRequiredConfig(config, "gcp.workload.identity.pool.id");
        String providerId = getRequiredConfig(config, "gcp.workload.identity.provider.id");
        String serviceAccountEmail = getRequiredConfig(config, "gcp.service.account.email");

        logger.info("Initializing Pub/Sub publisher with Workload Identity: {}/{}", projectId, topicId);
        logger.info("Using endpoint: {}", endpoint);
        logger.info("Workload Identity Pool: {}", poolId);
        logger.info("Workload Identity Provider: {}", providerId);
        logger.info("Service Account: {}", serviceAccountEmail);

        // Create credentials configuration JSON
        String credentialsJson = buildCredentialsJson(config, poolId, providerId, serviceAccountEmail);
        
        logger.info("Creating ExternalAccountCredentials from configuration");
        ExternalAccountCredentials credentials = ExternalAccountCredentials
                .fromJson(credentialsJson, /* transportFactory= */ null);

        // Build publisher with Workload Identity credentials
        publisher = Publisher.newBuilder(TopicName.of(projectId, topicId))
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .setEndpoint(endpoint)
                .build();
        
        logger.info("Publisher initialized successfully");
    }

    /**
     * Builds the ExternalAccountCredentials JSON configuration.
     * This mimics the structure of a workload identity configuration file.
     */
    private String buildCredentialsJson(Map<String, Object> config, String poolId, 
                                       String providerId, String serviceAccountEmail) {
        // Read subject token (from external OAuth provider)
        String subjectToken = getOptionalConfig(config, "external.subject.token");
        String subjectTokenFile = getOptionalConfig(config, "external.subject.token.file");
        String subjectTokenType = getOptionalConfig(config, "external.subject.token.type");
        
        if (subjectTokenType == null) {
            // Default to JWT token type (most common for OIDC providers)
            subjectTokenType = "urn:ietf:params:oauth:token-type:jwt";
        }

        // Construct the audience URL for the workload identity provider
        String audience = String.format(
            "//iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s/providers/%s",
            extractProjectNumber(poolId), extractPoolName(poolId), providerId
        );

        logger.info("Using audience: {}", audience);
        logger.info("Subject token type: {}", subjectTokenType);

        // Build the credential source
        String credentialSource;
        if (subjectToken != null) {
            logger.info("Using inline subject token (length: {})", subjectToken.length());
            // Inline token (less common, token is directly in config)
            credentialSource = String.format(
                "\"credential_source\": { \"format\": { \"type\": \"text\" }, \"text\": \"%s\" }",
                escapeJson(subjectToken)
            );
        } else if (subjectTokenFile != null) {
            logger.info("Using subject token from file: {}", subjectTokenFile);
            // Token from file (more common)
            credentialSource = String.format(
                "\"credential_source\": { \"file\": \"%s\", \"format\": { \"type\": \"text\" } }",
                escapeJson(subjectTokenFile)
            );
        } else {
            throw new IllegalArgumentException(
                "Either external.subject.token or external.subject.token.file must be provided"
            );
        }

        // Build the full JSON configuration
        return String.format(
            "{" +
            "  \"type\": \"external_account\"," +
            "  \"audience\": \"%s\"," +
            "  \"subject_token_type\": \"%s\"," +
            "  \"token_url\": \"https://sts.googleapis.com/v1/token\"," +
            "  \"service_account_impersonation_url\": \"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken\"," +
            "  %s" +
            "}",
            escapeJson(audience),
            escapeJson(subjectTokenType),
            escapeJson(serviceAccountEmail),
            credentialSource
        );
    }

    /**
     * Extracts project number from pool ID.
     * Pool ID format: projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_NAME
     */
    private String extractProjectNumber(String poolId) {
        String[] parts = poolId.split("/");
        if (parts.length >= 2 && "projects".equals(parts[0])) {
            return parts[1];
        }
        throw new IllegalArgumentException("Invalid workload identity pool ID format: " + poolId);
    }

    /**
     * Extracts pool name from pool ID.
     * Pool ID format: projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_NAME
     */
    private String extractPoolName(String poolId) {
        String[] parts = poolId.split("/");
        if (parts.length >= 6 && "workloadIdentityPools".equals(parts[4])) {
            return parts[5];
        }
        throw new IllegalArgumentException("Invalid workload identity pool ID format: " + poolId);
    }

    /**
     * Escapes special characters for JSON strings.
     */
    private String escapeJson(String str) {
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    private String getRequiredConfig(Map<String, Object> config, String key) {
        Object value = config.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Required config missing: " + key);
        }
        return value.toString();
    }

    private String getOptionalConfig(Map<String, Object> config, String key) {
        Object value = config.get(key);
        return value != null ? value.toString() : null;
    }

    @Override
    public void close() throws Exception {
        if (publisher != null) {
            publisher.shutdown();
            publisher.awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}

