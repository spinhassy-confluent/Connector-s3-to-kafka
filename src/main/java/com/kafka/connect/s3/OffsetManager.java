package com.kafka.connect.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages offset tracking for processed S3 objects
 */
public class OffsetManager {
    private static final Logger log = LoggerFactory.getLogger(OffsetManager.class);

    private final String offsetStorageKey;

    public OffsetManager(String offsetStorageKey) {
        this.offsetStorageKey = offsetStorageKey;
    }

    /**
     * Get offset for a specific S3 object key
     */
    public Map<String, Object> getOffset(String objectKey) {
        Map<String, Object> offset = new HashMap<>();
        offset.put("object_key", objectKey);
        offset.put("timestamp", System.currentTimeMillis());
        return offset;
    }

    /**
     * Get source partition for offset tracking
     */
    public Map<String, String> getSourcePartition(String objectKey) {
        Map<String, String> partition = new HashMap<>();
        partition.put("bucket", offsetStorageKey);
        partition.put("key", objectKey);
        return partition;
    }

    /**
     * Check if object has been processed (for incremental mode)
     */
    public boolean isProcessed(Map<String, Object> sourceOffset, String objectKey, long lastModified) {
        if (sourceOffset == null) {
            return false;
        }

        String processedKey = (String) sourceOffset.get("object_key");
        Long processedTimestamp = (Long) sourceOffset.get("last_modified");

        if (processedKey == null || !processedKey.equals(objectKey)) {
            return false;
        }

        // In incremental mode, reprocess if file was modified
        if (processedTimestamp != null && lastModified > processedTimestamp) {
            log.debug("Object {} was modified after last processing, will reprocess", objectKey);
            return false;
        }

        return true;
    }

    /**
     * Create source offset with last modified timestamp
     */
    public Map<String, Object> createOffset(String objectKey, long lastModified) {
        Map<String, Object> offset = new HashMap<>();
        offset.put("object_key", objectKey);
        offset.put("last_modified", lastModified);
        offset.put("processed_at", System.currentTimeMillis());
        return offset;
    }
}
