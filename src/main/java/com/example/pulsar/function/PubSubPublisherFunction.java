package com.example.pulsar.function;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar Function that publishes messages to Google Cloud Pub/Sub using Service Account credentials.
 * 
 * Required config: gcp.project.id, gcp.pubsub.topic, gcp.pubsub.endpoint
 * Auth config (choose one):
 *   - gcp.credentials.json: Service account JSON as string
 *   - gcp.credentials.base64: Service account JSON as base64-encoded string
 *   - gcp.credentials.path: Path to service account JSON file
 *   - (none): Uses Application Default Credentials
 */
public class PubSubPublisherFunction implements Function<byte[], Void> {

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
        
        // Read configuration
        String projectId = getRequiredConfig(config, "gcp.project.id");
        String topicId = getRequiredConfig(config, "gcp.pubsub.topic");
        String endpoint = getRequiredConfig(config, "gcp.pubsub.endpoint");
        
        logger.info("Initializing Pub/Sub publisher: {}/{}", projectId, topicId);
        logger.info("Using endpoint: {}", endpoint);

        // Build publisher with custom endpoint
        Publisher.Builder builder = Publisher.newBuilder(TopicName.of(projectId, topicId))
                .setEndpoint(endpoint);

        // Load credentials (supports multiple methods)
        GoogleCredentials credentials = loadCredentials(config);
        if (credentials != null) {
            builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }

        publisher = builder.build();
    }

    private GoogleCredentials loadCredentials(Map<String, Object> config) throws Exception {
        // Option 1: Raw JSON string
        String credentialsJson = getOptionalConfig(config, "gcp.credentials.json");
        if (credentialsJson != null) {
            logger.info("Using service account from JSON string");
            return GoogleCredentials.fromStream(
                    new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)));
        }

        // Option 2: Base64-encoded JSON
        String credentialsBase64 = getOptionalConfig(config, "gcp.credentials.base64");
        if (credentialsBase64 != null) {
            logger.info("Using service account from base64-encoded JSON");
            byte[] decodedBytes = Base64.getDecoder().decode(credentialsBase64);
            return GoogleCredentials.fromStream(new ByteArrayInputStream(decodedBytes));
        }

        // Option 3: File path
        String credentialsPath = getOptionalConfig(config, "gcp.credentials.path");
        if (credentialsPath != null) {
            logger.info("Using service account from file: {}", credentialsPath);
            return GoogleCredentials.fromStream(new FileInputStream(credentialsPath));
        }

        // Option 4: Application Default Credentials
        logger.info("Using Application Default Credentials");
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

