package com.example.pulsar.function;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar Function that publishes messages to Google Cloud Pub/Sub using OAuth2 authentication.
 * 
 * Required config: gcp.project.id, gcp.pubsub.topic
 * 
 * OAuth2 options (choose one):
 * 1. Access Token: gcp.oauth2.access.token (and optional gcp.oauth2.token.expiry)
 * 2. Refresh Token: gcp.oauth2.client.id, gcp.oauth2.client.secret, gcp.oauth2.refresh.token
 * 
 * Get credentials: gcloud auth application-default login
 * Then see: ~/.config/gcloud/application_default_credentials.json
 */
public class PubSubPublisherOAuth2Function implements Function<byte[], Void> {

    private Publisher publisher;
    private Logger logger;

    @Override
    public Void process(byte[] input, Context context) throws Exception {
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

        // Publish and wait for result
        String messageId = publisher.publish(messageBuilder.build()).get();
        logger.info("Published message to Pub/Sub: {}", messageId);

        return null;
    }

    private void initializePublisher(Context context) throws Exception {
        logger = context.getLogger();
        Map<String, Object> config = context.getUserConfigMap();
        
        String projectId = getRequiredConfig(config, "gcp.project.id");
        String topicId = getRequiredConfig(config, "gcp.pubsub.topic");
        
        logger.info("Initializing Pub/Sub publisher with OAuth2: {}/{}", projectId, topicId);

        GoogleCredentials credentials = createOAuth2Credentials(config);
        if (credentials == null) {
            throw new IllegalArgumentException("OAuth2 credentials not configured");
        }

        publisher = Publisher.newBuilder(TopicName.of(projectId, topicId))
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
    }

    private GoogleCredentials createOAuth2Credentials(Map<String, Object> config) {
        // Method 1: Access Token (short-lived)
        String accessToken = getOptionalConfig(config, "gcp.oauth2.access.token");
        if (accessToken != null) {
            logger.info("Using OAuth2 access token");
            String expiryStr = getOptionalConfig(config, "gcp.oauth2.token.expiry");
            Date expiry = expiryStr != null ? new Date(Long.parseLong(expiryStr)) : null;
            return GoogleCredentials.create(new AccessToken(accessToken, expiry));
        }
        
        // Method 2: Refresh Token (long-lived)
        String clientId = getOptionalConfig(config, "gcp.oauth2.client.id");
        String clientSecret = getOptionalConfig(config, "gcp.oauth2.client.secret");
        String refreshToken = getOptionalConfig(config, "gcp.oauth2.refresh.token");
        
        if (clientId != null && clientSecret != null && refreshToken != null) {
            logger.info("Using OAuth2 refresh token");
            return UserCredentials.newBuilder()
                    .setClientId(clientId)
                    .setClientSecret(clientSecret)
                    .setRefreshToken(refreshToken)
                    .build();
        }
        
        return null;
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

