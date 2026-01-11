package com.kafka.connect.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main task class that reads from S3 and produces Kafka records
 */
public class S3SourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(S3SourceTask.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private S3SourceConnectorConfig config;
    private S3ClientWrapper s3Client;
    private FileFormatParser fileParser;
    private OffsetManager offsetManager;
    private AtomicBoolean running = new AtomicBoolean(false);
    private String continuationToken = null;
    private List<SourceRecord> recordBuffer = new ArrayList<>();
    private int taskId = 0;
    private long lastPollTime = 0;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting S3 Source Task");
        
        try {
            config = new S3SourceConnectorConfig(props);
            
            // Get task ID if available
            String taskIdStr = props.get("task.id");
            if (taskIdStr != null) {
                taskId = Integer.parseInt(taskIdStr);
            }

            s3Client = new S3ClientWrapper(config);
            fileParser = new FileFormatParser(config);
            offsetManager = new OffsetManager(config.getOffsetStorageKey());

            running.set(true);
            log.info("S3 Source Task started successfully. Task ID: {}, Bucket: {}, Topic: {}", 
                    taskId, config.getBucketName(), config.getTopic());
        } catch (Exception e) {
            log.error("Failed to start S3 Source Task", e);
            throw new RuntimeException("Failed to start task", e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (!running.get()) {
            return null;
        }

        // Return buffered records if available
        if (!recordBuffer.isEmpty()) {
            List<SourceRecord> records = new ArrayList<>(recordBuffer);
            recordBuffer.clear();
            return records;
        }

        // Check if we should wait before polling
        long currentTime = System.currentTimeMillis();
        long timeSinceLastPoll = currentTime - lastPollTime;
        if (lastPollTime > 0 && timeSinceLastPoll < config.getPollIntervalMs()) {
            long sleepTime = config.getPollIntervalMs() - timeSinceLastPoll;
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Sleep interrupted during poll interval");
                return null;
            }
        }

        try {
            // Poll S3 for new objects
            List<S3Object> objects = s3Client.listObjects(continuationToken);
            lastPollTime = System.currentTimeMillis();
            
            if (objects.isEmpty()) {
                // No new objects, reset continuation token and return empty
                continuationToken = null;
                return Collections.emptyList();
            }

            // Process objects and create records
            List<SourceRecord> records = new ArrayList<>();
            int processedCount = 0;

            for (S3Object s3Object : objects) {
                if (processedCount >= config.getMaxObjectsPerPoll()) {
                    break;
                }

                try {
                    List<SourceRecord> objectRecords = processObject(s3Object);
                    records.addAll(objectRecords);
                    processedCount++;
                } catch (Exception e) {
                    handleError(s3Object.key(), e, records);
                }
            }

            // Note: In a production implementation, you would get the continuation token
            // from the ListObjectsV2Response and store it for the next poll
            // For now, we reset it to start from the beginning each time
            continuationToken = null;

            // Batch records if configured
            if (records.size() > config.getBatchSize()) {
                recordBuffer.addAll(records.subList(config.getBatchSize(), records.size()));
                return records.subList(0, config.getBatchSize());
            }

            return records;

        } catch (Exception e) {
            log.error("Error during poll", e);
            if ("fail".equals(config.getErrorHandling())) {
                throw new RuntimeException("Poll failed", e);
            }
            return Collections.emptyList();
        }
    }

    private List<SourceRecord> processObject(S3Object s3Object) {
        if (s3Object == null) {
            log.warn("Received null S3 object, skipping");
            return Collections.emptyList();
        }
        
        String objectKey = s3Object.key();
        if (objectKey == null || objectKey.isEmpty()) {
            log.warn("S3 object has null or empty key, skipping");
            return Collections.emptyList();
        }
        
        // Get last modified timestamp
        long lastModified;
        if (s3Object.lastModified() != null) {
            lastModified = s3Object.lastModified().toEpochMilli();
        } else {
            log.warn("S3 object {} has null lastModified timestamp, using current time. This may affect offset tracking.", objectKey);
            lastModified = System.currentTimeMillis();
        }

        log.debug("Processing S3 object: {}", objectKey);

        // Check if already processed (incremental mode)
        if ("incremental".equals(config.getReadMode())) {
            Map<String, Object> sourceOffset = context.offsetStorageReader()
                    .offset(offsetManager.getSourcePartition(objectKey));
            
            if (offsetManager.isProcessed(sourceOffset, objectKey, lastModified)) {
                log.debug("Object {} already processed, skipping", objectKey);
                return Collections.emptyList();
            }
        }

        // Get object content
        byte[] content = s3Client.getObjectContent(objectKey);
        
        if (content == null || content.length == 0) {
            log.warn("Object {} has no content, skipping", objectKey);
            return Collections.emptyList();
        }
        
        // Parse content based on file format
        List<Map<String, Object>> parsedRecords = fileParser.parse(content, objectKey);

        // Convert to SourceRecords
        List<SourceRecord> sourceRecords = new ArrayList<>();
        for (Map<String, Object> record : parsedRecords) {
            if (record == null) {
                continue;
            }
            
            // Add metadata if configured
            if (config.getIncludeMetadata()) {
                String prefix = config.getMetadataFieldPrefix();
                record.put(prefix + "key", objectKey);
                record.put(prefix + "size", s3Object.size());
                record.put(prefix + "last_modified", s3Object.lastModified() != null ? s3Object.lastModified().toString() : "unknown");
                record.put(prefix + "etag", s3Object.eTag() != null ? s3Object.eTag() : "unknown");
            }

            // Create Kafka record
            SourceRecord sourceRecord = createSourceRecord(record, objectKey, lastModified);
            sourceRecords.add(sourceRecord);
        }

        log.info("Processed object {}: {} records created", objectKey, sourceRecords.size());
        return sourceRecords;
    }

    private SourceRecord createSourceRecord(Map<String, Object> record, String objectKey, long lastModified) {
        // Determine topic
        String topic = config.getTopic();

        // Determine partition (if configured)
        Integer partition = null;
        if (config.getPartitionField() != null && !config.getPartitionField().isEmpty()) {
            Object partitionValue = record.get(config.getPartitionField());
            if (partitionValue != null) {
                partition = Math.abs(partitionValue.hashCode()) % 100; // Simple partitioning
            }
        }

        // Determine key
        Object key = null;
        if (config.getKeyField() != null && !config.getKeyField().isEmpty()) {
            key = record.get(config.getKeyField());
        }
        if (key == null) {
            key = objectKey; // Default to object key
        }

        // Serialize value
        String value;
        try {
            value = objectMapper.writeValueAsString(record);
        } catch (Exception e) {
            log.error("Failed to serialize record", e);
            value = record.toString();
        }

        // Create source partition and offset
        Map<String, String> sourcePartition = offsetManager.getSourcePartition(objectKey);
        Map<String, Object> sourceOffset = offsetManager.createOffset(objectKey, lastModified);

        // Create schema (using simple string schema for flexibility)
        Schema keySchema = Schema.STRING_SCHEMA;
        Schema valueSchema = Schema.STRING_SCHEMA;

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                partition,
                keySchema,
                key != null ? key.toString() : null,
                valueSchema,
                value
        );
    }

    private void handleError(String objectKey, Exception e, List<SourceRecord> records) {
        // Sanitize error message to avoid exposing sensitive information
        String sanitizedMessage = sanitizeErrorMessage(e.getMessage());
        log.error("Error processing object: {}, error: {}", objectKey, sanitizedMessage);

        if ("fail".equals(config.getErrorHandling())) {
            throw new RuntimeException("Failed to process object: " + objectKey, e);
        } else if ("skip".equals(config.getErrorHandling())) {
            log.warn("Skipping object {} due to error", objectKey);
            
            // Send to dead letter topic if configured
            String dlqTopic = config.getDeadLetterTopic();
            if (dlqTopic != null && !dlqTopic.isEmpty()) {
                try {
                    Map<String, Object> errorRecord = new HashMap<>();
                    errorRecord.put("error", sanitizedMessage);
                    errorRecord.put("error_type", e.getClass().getSimpleName());
                    errorRecord.put("object_key", objectKey);
                    errorRecord.put("timestamp", System.currentTimeMillis());
                    
                    SourceRecord dlqRecord = new SourceRecord(
                            offsetManager.getSourcePartition(objectKey),
                            offsetManager.getOffset(objectKey),
                            dlqTopic,
                            null,
                            Schema.STRING_SCHEMA,
                            objectKey,
                            Schema.STRING_SCHEMA,
                            objectMapper.writeValueAsString(errorRecord)
                    );
                    records.add(dlqRecord);
                } catch (Exception ex) {
                    log.error("Failed to create dead letter record", ex);
                }
            }
        }
    }
    
    /**
     * Sanitize error messages to prevent sensitive information exposure
     */
    private String sanitizeErrorMessage(String message) {
        if (message == null) {
            return "Unknown error";
        }
        
        // Remove potential access keys, secrets, and credentials from error messages
        String sanitized = message.replaceAll("(?i)(aws[_-]?access[_-]?key[_-]?id|access[_-]?key)[:\\s=]+[A-Za-z0-9]{16,}", "$1=***");
        sanitized = sanitized.replaceAll("(?i)(aws[_-]?secret[_-]?access[_-]?key|secret[_-]?key)[:\\s=]+[A-Za-z0-9/+=]{40}", "$1=***");
        sanitized = sanitized.replaceAll("(?i)(password|passwd|pwd)[:\\s=]+\\S+", "$1=***");
        sanitized = sanitized.replaceAll("(?i)(session[_-]?token|token)[:\\s=]+[A-Za-z0-9/+=]{16,}", "$1=***");
        
        return sanitized;
    }

    @Override
    public void stop() {
        log.info("Stopping S3 Source Task");
        running.set(false);
        
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (Exception e) {
                log.error("Error closing S3 client", e);
            }
        }
        
        log.info("S3 Source Task stopped");
    }
}
