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

import org.apache.pulsar.client.api.schema.GenericRecord; // Use this one!
import org.apache.pulsar.common.schema.KeyValue;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar Function to generically consume DataStax CDC messages from Cassandra CDC Pulsar topics
 * (key + value are Avro) and publish JSON-wrapped record to Google Pub/Sub using Service Account credentials.
 * 
 * Follows config rules from PubSubPublisherFunction:
 *   - gcp.project.id, gcp.pubsub.topic, gcp.pubsub.endpoint
 *   - authentication config as per PubSubPublisherFunction
 *
 * This is a generic CDC handler: KeyValue<GenericRecord, GenericRecord> input.
 */
public class GenericCdcToPubSubFunction implements Function<GenericRecord, Void> {
    private Publisher publisher;
    private Logger logger;
    
    @Override
    public Void process(GenericRecord input, Context context) throws Exception {
        // Under AUTO_CONSUME with a KeyValue topic, 
        // the input is a GenericRecord where the Native Object is a KeyValue pair.
        Object nativeObject = input.getNativeObject();
        
        if (!(nativeObject instanceof KeyValue)) {
            throw new RuntimeException("Expected KeyValue object, but got: " + nativeObject.getClass());
        }

        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) nativeObject;
        
        GenericRecord keyRecord = kv.getKey();
        GenericRecord valueRecord = kv.getValue();

        // Convert them to JSON strings
        String keyJson = recordToJson(keyRecord);
        String valueJson = recordToJson(valueRecord);
                
        if (logger == null) logger = context.getLogger();
        logger.info("GenericCdcToPubSubFunction.process() called");
        logger.info("Raw input KeyValue message: {}", input);
        if (publisher == null) initializePublisher(context);

        // Prepare composite Pub/Sub payload as JSON
        Map<String, Object> payload = new HashMap<>();
        payload.put("cdcKey", keyJson);
        payload.put("cdcValue", valueJson);
        // Optionally, add metadata from Pulsar record properties
        Map<String, String> properties = context.getCurrentRecord().getProperties();
        if (properties != null && !properties.isEmpty()) {
            payload.put("pulsarProperties", properties);
        }

        String jsonPayload = mapToJson(payload);

        // Print the message prior to publishing to Pub/Sub
        logger.info("CDC message JSON payload before publish: {}", jsonPayload);
        
        // Build Pub/Sub message
        PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(jsonPayload, StandardCharsets.UTF_8));
        // Add Pulsar properties as Pub/Sub message attributes
        if (properties != null && !properties.isEmpty()) {
            messageBuilder.putAllAttributes(properties);
        }

        String messageId = publisher.publish(messageBuilder.build()).get();
        logger.info("Published CDC message to Pub/Sub: {}", messageId);
        return null;
    }

    private String recordToJson(GenericRecord record) {
        if (record == null) return "null";
        
        // Pulsar's GenericRecord native object for Avro is a GenericAvroRecord
        // Its toString() method produces valid JSON in most cases
        return record.getNativeObject().toString();
    }

    private void initializePublisher(Context context) throws Exception {
        logger = context.getLogger();
        Map<String, Object> config = context.getUserConfigMap();
        String projectId = getRequiredConfig(config, "gcp.project.id");
        String topicId = getRequiredConfig(config, "gcp.pubsub.topic");
        String endpoint = getRequiredConfig(config, "gcp.pubsub.endpoint");

        logger.info("Initializing Pub/Sub publisher: {}/{}", projectId, topicId);
        logger.info("Using endpoint: {}", endpoint);

        Publisher.Builder builder = Publisher.newBuilder(TopicName.of(projectId, topicId))
                .setEndpoint(endpoint);
        GoogleCredentials credentials = loadCredentials(config);
        if (credentials != null) {
            builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        publisher = builder.build();
    }

    private GoogleCredentials loadCredentials(Map<String, Object> config) throws Exception {
        String credentialsJson = getOptionalConfig(config, "gcp.credentials.json");
        if (credentialsJson != null) {
            logger.info("Using service account from JSON string");
            return GoogleCredentials.fromStream(
                    new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8)))
                        .createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
        }
        String credentialsBase64 = getOptionalConfig(config, "gcp.credentials.base64");
        if (credentialsBase64 != null) {
            logger.info("Using service account from base64-encoded JSON");
            byte[] decodedBytes = Base64.getDecoder().decode(credentialsBase64);
            GoogleCredentials creds = GoogleCredentials.fromStream(new ByteArrayInputStream(decodedBytes))
                    .createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
            logger.info("Credentials Type: " + creds.getClass().getName());
            return creds;
        }
        String credentialsPath = getOptionalConfig(config, "gcp.credentials.path");
        if (credentialsPath != null) {
            logger.info("Using service account from file: {}", credentialsPath);
            return GoogleCredentials.fromStream(new FileInputStream(credentialsPath))
                    .createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
        }
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



    // Simple manual JSON (for demo/testing). For production, use Jackson/Gson!
    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append('"').append(entry.getKey()).append('"').append(":");
            if (entry.getValue() == null) {
                sb.append("null");
            } else if (entry.getValue() instanceof String) {
                sb.append('"').append(entry.getValue().toString().replace("\"", "\\\"")).append('"');
            } else {
                sb.append(entry.getValue().toString());
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        if (publisher != null) {
            publisher.shutdown();
            publisher.awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}

