package com.kafka.connect.s3;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

/**
 * Configuration class for S3 Source Connector
 * Defines all mandatory and optional configuration properties
 */
public class S3SourceConnectorConfig extends AbstractConfig {

    // Mandatory Configuration Properties
    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    public static final String S3_BUCKET_NAME_DOC = "Name of the S3 bucket to read from";

    public static final String TOPIC_CONFIG = "topic";
    public static final String TOPIC_DOC = "Kafka topic to write data to";

    // Optional Configuration Properties
    public static final String S3_REGION_CONFIG = "s3.region";
    public static final String S3_REGION_DEFAULT = "us-east-1";
    public static final String S3_REGION_DOC = "AWS region where the S3 bucket is located";
    
    public static final String S3_ENDPOINT_URL_CONFIG = "s3.endpoint.url";
    public static final String S3_ENDPOINT_URL_DEFAULT = "";
    public static final String S3_ENDPOINT_URL_DOC = "Custom S3 endpoint URL for S3-compatible storage (e.g., MinIO, Ceph). Leave empty for AWS S3.";
    
    public static final String S3_PATH_STYLE_ACCESS_CONFIG = "s3.path.style.access";
    public static final boolean S3_PATH_STYLE_ACCESS_DEFAULT = false;
    public static final String S3_PATH_STYLE_ACCESS_DOC = "Use path-style access for S3 (required for some S3-compatible storage). Default is virtual-hosted-style.";

    public static final String S3_PREFIX_CONFIG = "s3.prefix";
    public static final String S3_PREFIX_DEFAULT = "";
    public static final String S3_PREFIX_DOC = "Prefix to filter S3 objects (e.g., 'folder/subfolder/')";

    public static final String S3_SUFFIX_CONFIG = "s3.suffix";
    public static final String S3_SUFFIX_DEFAULT = "";
    public static final String S3_SUFFIX_DOC = "Suffix to filter S3 objects (e.g., '.json', '.csv')";

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_ACCESS_KEY_ID_DEFAULT = "";
    public static final String AWS_ACCESS_KEY_ID_DOC = "AWS access key ID. If not provided, uses default credential chain";

    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_SECRET_ACCESS_KEY_DEFAULT = "";
    public static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS secret access key. If not provided, uses default credential chain";

    public static final String AWS_SESSION_TOKEN_CONFIG = "aws.session.token";
    public static final String AWS_SESSION_TOKEN_DEFAULT = "";
    public static final String AWS_SESSION_TOKEN_DOC = "AWS session token for temporary credentials";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    public static final long POLL_INTERVAL_MS_DEFAULT = 60000L;
    public static final String POLL_INTERVAL_MS_DOC = "Interval in milliseconds between polling S3 for new objects";

    public static final String MAX_OBJECTS_PER_POLL_CONFIG = "max.objects.per.poll";
    public static final int MAX_OBJECTS_PER_POLL_DEFAULT = 100;
    public static final String MAX_OBJECTS_PER_POLL_DOC = "Maximum number of S3 objects to process in a single poll";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final int BATCH_SIZE_DEFAULT = 1000;
    public static final String BATCH_SIZE_DOC = "Number of records to batch before sending to Kafka";

    public static final String OFFSET_STORAGE_KEY_CONFIG = "offset.storage.key";
    public static final String OFFSET_STORAGE_KEY_DEFAULT = "s3-source-connector-offset";
    public static final String OFFSET_STORAGE_KEY_DOC = "Key used to store offset information in Kafka Connect";

    public static final String READ_MODE_CONFIG = "read.mode";
    public static final String READ_MODE_DEFAULT = "full";
    public static final String READ_MODE_DOC = "Read mode: 'full' (read entire file) or 'incremental' (only new/changed files)";

    public static final String FILE_FORMAT_CONFIG = "file.format";
    public static final String FILE_FORMAT_DEFAULT = "json";
    public static final String FILE_FORMAT_DOC = "File format: 'json', 'csv', 'text', 'avro', or 'binary'";

    public static final String CSV_DELIMITER_CONFIG = "csv.delimiter";
    public static final String CSV_DELIMITER_DEFAULT = ",";
    public static final String CSV_DELIMITER_DOC = "Delimiter for CSV files";

    public static final String CSV_HEADER_CONFIG = "csv.header";
    public static final boolean CSV_HEADER_DEFAULT = true;
    public static final String CSV_HEADER_DOC = "Whether CSV files have a header row";

    public static final String JSON_ARRAY_MODE_CONFIG = "json.array.mode";
    public static final boolean JSON_ARRAY_MODE_DEFAULT = false;
    public static final String JSON_ARRAY_MODE_DOC = "If true, treats each file as a JSON array and splits into individual records";

    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final int MAX_RETRIES_DEFAULT = 3;
    public static final String MAX_RETRIES_DOC = "Maximum number of retries for failed operations";

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final long RETRY_BACKOFF_MS_DEFAULT = 1000L;
    public static final String RETRY_BACKOFF_MS_DOC = "Backoff time in milliseconds between retries";

    public static final String CONNECT_TIMEOUT_MS_CONFIG = "connect.timeout.ms";
    public static final int CONNECT_TIMEOUT_MS_DEFAULT = 10000;
    public static final String CONNECT_TIMEOUT_MS_DOC = "Connection timeout in milliseconds";

    public static final String SOCKET_TIMEOUT_MS_CONFIG = "socket.timeout.ms";
    public static final int SOCKET_TIMEOUT_MS_DEFAULT = 50000;
    public static final String SOCKET_TIMEOUT_MS_DOC = "Socket timeout in milliseconds";

    public static final String INCLUDE_METADATA_CONFIG = "include.metadata";
    public static final boolean INCLUDE_METADATA_DEFAULT = true;
    public static final String INCLUDE_METADATA_DOC = "Include S3 object metadata (key, size, lastModified) in Kafka records";

    public static final String METADATA_FIELD_PREFIX_CONFIG = "metadata.field.prefix";
    public static final String METADATA_FIELD_PREFIX_DEFAULT = "__s3_";
    public static final String METADATA_FIELD_PREFIX_DOC = "Prefix for metadata fields in Kafka records";

    public static final String FILTER_BY_LAST_MODIFIED_CONFIG = "filter.by.last.modified";
    public static final boolean FILTER_BY_LAST_MODIFIED_DEFAULT = false;
    public static final String FILTER_BY_LAST_MODIFIED_DOC = "Filter objects by last modified time";

    public static final String LAST_MODIFIED_AFTER_CONFIG = "last.modified.after";
    public static final String LAST_MODIFIED_AFTER_DEFAULT = "";
    public static final String LAST_MODIFIED_AFTER_DOC = "Only process objects modified after this timestamp (ISO 8601 format)";

    public static final String LAST_MODIFIED_BEFORE_CONFIG = "last.modified.before";
    public static final String LAST_MODIFIED_BEFORE_DEFAULT = "";
    public static final String LAST_MODIFIED_BEFORE_DOC = "Only process objects modified before this timestamp (ISO 8601 format)";

    public static final String MIN_OBJECT_SIZE_CONFIG = "min.object.size";
    public static final long MIN_OBJECT_SIZE_DEFAULT = 0L;
    public static final String MIN_OBJECT_SIZE_DOC = "Minimum object size in bytes to process";

    public static final String MAX_OBJECT_SIZE_CONFIG = "max.object.size";
    public static final long MAX_OBJECT_SIZE_DEFAULT = Long.MAX_VALUE;
    public static final String MAX_OBJECT_SIZE_DOC = "Maximum object size in bytes to process";

    public static final String TASKS_MAX_CONFIG = "tasks.max";
    public static final int TASKS_MAX_DEFAULT = 1;
    public static final String TASKS_MAX_DOC = "Maximum number of tasks for this connector";

    public static final String ERROR_HANDLING_CONFIG = "error.handling";
    public static final String ERROR_HANDLING_DEFAULT = "fail";
    public static final String ERROR_HANDLING_DOC = "Error handling strategy: 'fail' (stop on error) or 'skip' (skip failed objects)";

    public static final String DEAD_LETTER_TOPIC_CONFIG = "dead.letter.topic";
    public static final String DEAD_LETTER_TOPIC_DEFAULT = "";
    public static final String DEAD_LETTER_TOPIC_DOC = "Topic to send failed records to (if error.handling is 'skip')";

    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    public static final String COMPRESSION_TYPE_DEFAULT = "none";
    public static final String COMPRESSION_TYPE_DOC = "Compression type: 'none', 'gzip', 'snappy', 'lz4', 'zstd'";

    public static final String PARTITION_FIELD_CONFIG = "partition.field";
    public static final String PARTITION_FIELD_DEFAULT = "";
    public static final String PARTITION_FIELD_DOC = "Field name to use for partitioning Kafka records";

    public static final String KEY_FIELD_CONFIG = "key.field";
    public static final String KEY_FIELD_DEFAULT = "";
    public static final String KEY_FIELD_DOC = "Field name to use as Kafka record key";

    public static final String VALUE_SERIALIZER_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER_DOC = "Serializer class for record values";

    public static final String KEY_SERIALIZER_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEY_SERIALIZER_DOC = "Serializer class for record keys";

    public static ConfigDef config() {
        return new ConfigDef()
                // Mandatory configurations
                .define(S3_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, S3_BUCKET_NAME_DOC)
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, TOPIC_DOC)

                // AWS Configuration
                .define(S3_REGION_CONFIG, ConfigDef.Type.STRING, S3_REGION_DEFAULT,
                        ConfigDef.Importance.MEDIUM, S3_REGION_DOC)
                .define(S3_ENDPOINT_URL_CONFIG, ConfigDef.Type.STRING, S3_ENDPOINT_URL_DEFAULT,
                        ConfigDef.Importance.LOW, S3_ENDPOINT_URL_DOC)
                .define(S3_PATH_STYLE_ACCESS_CONFIG, ConfigDef.Type.BOOLEAN, S3_PATH_STYLE_ACCESS_DEFAULT,
                        ConfigDef.Importance.LOW, S3_PATH_STYLE_ACCESS_DOC)
                .define(AWS_ACCESS_KEY_ID_CONFIG, ConfigDef.Type.STRING, AWS_ACCESS_KEY_ID_DEFAULT,
                        ConfigDef.Importance.MEDIUM, AWS_ACCESS_KEY_ID_DOC)
                .define(AWS_SECRET_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD, AWS_SECRET_ACCESS_KEY_DEFAULT,
                        ConfigDef.Importance.MEDIUM, AWS_SECRET_ACCESS_KEY_DOC)
                .define(AWS_SESSION_TOKEN_CONFIG, ConfigDef.Type.PASSWORD, AWS_SESSION_TOKEN_DEFAULT,
                        ConfigDef.Importance.LOW, AWS_SESSION_TOKEN_DOC)

                // S3 Filtering
                .define(S3_PREFIX_CONFIG, ConfigDef.Type.STRING, S3_PREFIX_DEFAULT,
                        ConfigDef.Importance.MEDIUM, S3_PREFIX_DOC)
                .define(S3_SUFFIX_CONFIG, ConfigDef.Type.STRING, S3_SUFFIX_DEFAULT,
                        ConfigDef.Importance.MEDIUM, S3_SUFFIX_DOC)
                .define(FILTER_BY_LAST_MODIFIED_CONFIG, ConfigDef.Type.BOOLEAN, FILTER_BY_LAST_MODIFIED_DEFAULT,
                        ConfigDef.Importance.LOW, FILTER_BY_LAST_MODIFIED_DOC)
                .define(LAST_MODIFIED_AFTER_CONFIG, ConfigDef.Type.STRING, LAST_MODIFIED_AFTER_DEFAULT,
                        ConfigDef.Importance.LOW, LAST_MODIFIED_AFTER_DOC)
                .define(LAST_MODIFIED_BEFORE_CONFIG, ConfigDef.Type.STRING, LAST_MODIFIED_BEFORE_DEFAULT,
                        ConfigDef.Importance.LOW, LAST_MODIFIED_BEFORE_DOC)
                .define(MIN_OBJECT_SIZE_CONFIG, ConfigDef.Type.LONG, MIN_OBJECT_SIZE_DEFAULT,
                        ConfigDef.Importance.LOW, MIN_OBJECT_SIZE_DOC)
                .define(MAX_OBJECT_SIZE_CONFIG, ConfigDef.Type.LONG, MAX_OBJECT_SIZE_DEFAULT,
                        ConfigDef.Importance.LOW, MAX_OBJECT_SIZE_DOC)

                // Polling Configuration
                .define(POLL_INTERVAL_MS_CONFIG, ConfigDef.Type.LONG, POLL_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM, POLL_INTERVAL_MS_DOC)
                .define(MAX_OBJECTS_PER_POLL_CONFIG, ConfigDef.Type.INT, MAX_OBJECTS_PER_POLL_DEFAULT,
                        ConfigDef.Importance.MEDIUM, MAX_OBJECTS_PER_POLL_DOC)
                .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT,
                        ConfigDef.Importance.MEDIUM, BATCH_SIZE_DOC)

                // Offset and Read Mode
                .define(OFFSET_STORAGE_KEY_CONFIG, ConfigDef.Type.STRING, OFFSET_STORAGE_KEY_DEFAULT,
                        ConfigDef.Importance.LOW, OFFSET_STORAGE_KEY_DOC)
                .define(READ_MODE_CONFIG, ConfigDef.Type.STRING, READ_MODE_DEFAULT,
                        ConfigDef.ValidString.in("full", "incremental"),
                        ConfigDef.Importance.MEDIUM, READ_MODE_DOC)

                // File Format Configuration
                .define(FILE_FORMAT_CONFIG, ConfigDef.Type.STRING, FILE_FORMAT_DEFAULT,
                        ConfigDef.ValidString.in("json", "csv", "text", "avro", "binary"),
                        ConfigDef.Importance.HIGH, FILE_FORMAT_DOC)
                .define(CSV_DELIMITER_CONFIG, ConfigDef.Type.STRING, CSV_DELIMITER_DEFAULT,
                        ConfigDef.Importance.MEDIUM, CSV_DELIMITER_DOC)
                .define(CSV_HEADER_CONFIG, ConfigDef.Type.BOOLEAN, CSV_HEADER_DEFAULT,
                        ConfigDef.Importance.MEDIUM, CSV_HEADER_DOC)
                .define(JSON_ARRAY_MODE_CONFIG, ConfigDef.Type.BOOLEAN, JSON_ARRAY_MODE_DEFAULT,
                        ConfigDef.Importance.MEDIUM, JSON_ARRAY_MODE_DOC)

                // Error Handling and Retry
                .define(MAX_RETRIES_CONFIG, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT,
                        ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
                .define(RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, RETRY_BACKOFF_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC)
                .define(ERROR_HANDLING_CONFIG, ConfigDef.Type.STRING, ERROR_HANDLING_DEFAULT,
                        ConfigDef.ValidString.in("fail", "skip"),
                        ConfigDef.Importance.MEDIUM, ERROR_HANDLING_DOC)
                .define(DEAD_LETTER_TOPIC_CONFIG, ConfigDef.Type.STRING, DEAD_LETTER_TOPIC_DEFAULT,
                        ConfigDef.Importance.LOW, DEAD_LETTER_TOPIC_DOC)

                // Timeout Configuration
                .define(CONNECT_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, CONNECT_TIMEOUT_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM, CONNECT_TIMEOUT_MS_DOC)
                .define(SOCKET_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, SOCKET_TIMEOUT_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM, SOCKET_TIMEOUT_MS_DOC)

                // Metadata Configuration
                .define(INCLUDE_METADATA_CONFIG, ConfigDef.Type.BOOLEAN, INCLUDE_METADATA_DEFAULT,
                        ConfigDef.Importance.LOW, INCLUDE_METADATA_DOC)
                .define(METADATA_FIELD_PREFIX_CONFIG, ConfigDef.Type.STRING, METADATA_FIELD_PREFIX_DEFAULT,
                        ConfigDef.Importance.LOW, METADATA_FIELD_PREFIX_DOC)

                // Kafka Configuration
                .define(TASKS_MAX_CONFIG, ConfigDef.Type.INT, TASKS_MAX_DEFAULT,
                        ConfigDef.Importance.MEDIUM, TASKS_MAX_DOC)
                .define(COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, COMPRESSION_TYPE_DEFAULT,
                        ConfigDef.ValidString.in("none", "gzip", "snappy", "lz4", "zstd"),
                        ConfigDef.Importance.LOW, COMPRESSION_TYPE_DOC)
                .define(PARTITION_FIELD_CONFIG, ConfigDef.Type.STRING, PARTITION_FIELD_DEFAULT,
                        ConfigDef.Importance.LOW, PARTITION_FIELD_DOC)
                .define(KEY_FIELD_CONFIG, ConfigDef.Type.STRING, KEY_FIELD_DEFAULT,
                        ConfigDef.Importance.LOW, KEY_FIELD_DOC)
                .define(VALUE_SERIALIZER_CONFIG, ConfigDef.Type.STRING, VALUE_SERIALIZER_DEFAULT,
                        ConfigDef.Importance.LOW, VALUE_SERIALIZER_DOC)
                .define(KEY_SERIALIZER_CONFIG, ConfigDef.Type.STRING, KEY_SERIALIZER_DEFAULT,
                        ConfigDef.Importance.LOW, KEY_SERIALIZER_DOC);
    }

    public S3SourceConnectorConfig(Map<String, String> props) {
        super(config(), props);
        validate();
    }

    private void validate() {
        String bucketName = getString(S3_BUCKET_NAME_CONFIG);
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new ConfigException(S3_BUCKET_NAME_CONFIG + " is required and cannot be empty");
        }
        
        // Validate bucket name format according to S3 bucket naming rules
        if (!isValidBucketName(bucketName)) {
            throw new ConfigException(S3_BUCKET_NAME_CONFIG + " contains invalid characters. " +
                    "Bucket names must be between 3 and 63 characters long, " +
                    "can contain lowercase letters, numbers, hyphens, and periods.");
        }

        String topic = getString(TOPIC_CONFIG);
        if (topic == null || topic.trim().isEmpty()) {
            throw new ConfigException(TOPIC_CONFIG + " is required and cannot be empty");
        }

        long minSize = getLong(MIN_OBJECT_SIZE_CONFIG);
        long maxSize = getLong(MAX_OBJECT_SIZE_CONFIG);
        if (minSize > maxSize) {
            throw new ConfigException(MIN_OBJECT_SIZE_CONFIG + " cannot be greater than " + MAX_OBJECT_SIZE_CONFIG);
        }
        
        if (minSize < 0) {
            throw new ConfigException(MIN_OBJECT_SIZE_CONFIG + " cannot be negative");
        }

        String errorHandling = getString(ERROR_HANDLING_CONFIG);
        if ("skip".equals(errorHandling)) {
            String dlqTopic = getString(DEAD_LETTER_TOPIC_CONFIG);
            if (dlqTopic == null || dlqTopic.trim().isEmpty()) {
                throw new ConfigException(DEAD_LETTER_TOPIC_CONFIG + " is required when " + ERROR_HANDLING_CONFIG + " is 'skip'");
            }
        }
        
        // Validate timestamp formats if provided
        if (getBoolean(FILTER_BY_LAST_MODIFIED_CONFIG)) {
            String lastModifiedAfter = getString(LAST_MODIFIED_AFTER_CONFIG);
            if (lastModifiedAfter != null && !lastModifiedAfter.isEmpty()) {
                try {
                    java.time.Instant.parse(lastModifiedAfter);
                } catch (Exception e) {
                    throw new ConfigException(LAST_MODIFIED_AFTER_CONFIG + " must be in ISO 8601 format (e.g., 2024-01-01T00:00:00Z)");
                }
            }
            
            String lastModifiedBefore = getString(LAST_MODIFIED_BEFORE_CONFIG);
            if (lastModifiedBefore != null && !lastModifiedBefore.isEmpty()) {
                try {
                    java.time.Instant.parse(lastModifiedBefore);
                } catch (Exception e) {
                    throw new ConfigException(LAST_MODIFIED_BEFORE_CONFIG + " must be in ISO 8601 format (e.g., 2024-01-01T00:00:00Z)");
                }
            }
        }
        
        // Validate positive values
        if (getInt(MAX_RETRIES_CONFIG) < 0) {
            throw new ConfigException(MAX_RETRIES_CONFIG + " cannot be negative");
        }
        
        if (getLong(RETRY_BACKOFF_MS_CONFIG) < 0) {
            throw new ConfigException(RETRY_BACKOFF_MS_CONFIG + " cannot be negative");
        }
        
        if (getInt(CONNECT_TIMEOUT_MS_CONFIG) <= 0) {
            throw new ConfigException(CONNECT_TIMEOUT_MS_CONFIG + " must be positive");
        }
        
        if (getInt(SOCKET_TIMEOUT_MS_CONFIG) <= 0) {
            throw new ConfigException(SOCKET_TIMEOUT_MS_CONFIG + " must be positive");
        }
        
        if (getInt(MAX_OBJECTS_PER_POLL_CONFIG) <= 0) {
            throw new ConfigException(MAX_OBJECTS_PER_POLL_CONFIG + " must be positive");
        }
        
        if (getInt(BATCH_SIZE_CONFIG) <= 0) {
            throw new ConfigException(BATCH_SIZE_CONFIG + " must be positive");
        }
        
        if (getLong(POLL_INTERVAL_MS_CONFIG) < 0) {
            throw new ConfigException(POLL_INTERVAL_MS_CONFIG + " cannot be negative");
        }
    }
    
    /**
     * Validates S3 bucket name according to AWS naming rules
     */
    private boolean isValidBucketName(String bucketName) {
        if (bucketName.length() < 3 || bucketName.length() > 63) {
            return false;
        }
        
        // Bucket names must be all lowercase
        if (!bucketName.equals(bucketName.toLowerCase())) {
            return false;
        }
        
        // Bucket names must start and end with lowercase letter or number
        char firstChar = bucketName.charAt(0);
        char lastChar = bucketName.charAt(bucketName.length() - 1);
        if (!(Character.isLowerCase(firstChar) || Character.isDigit(firstChar)) || 
            !(Character.isLowerCase(lastChar) || Character.isDigit(lastChar))) {
            return false;
        }
        
        // Check for valid characters: lowercase letters, numbers, hyphens, periods
        for (char c : bucketName.toCharArray()) {
            if (!Character.isLowerCase(c) && !Character.isDigit(c) && c != '-' && c != '.') {
                return false;
            }
        }
        
        // Bucket names must not be formatted as IP addresses (0-255.0-255.0-255.0-255)
        if (bucketName.matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$")) {
            // Validate that it's actually an IP address pattern
            String[] parts = bucketName.split("\\.");
            if (parts.length == 4) {
                try {
                    for (String part : parts) {
                        int value = Integer.parseInt(part);
                        if (value >= 0 && value <= 255) {
                            // This looks like a valid IP address, reject it
                            return false;
                        }
                    }
                } catch (NumberFormatException e) {
                    // Not a valid number, so it's fine
                }
            }
        }
        
        return true;
    }

    // Getters for all configuration properties
    public String getBucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }

    public String getTopic() {
        return getString(TOPIC_CONFIG);
    }

    public String getRegion() {
        return getString(S3_REGION_CONFIG);
    }

    public String getPrefix() {
        return getString(S3_PREFIX_CONFIG);
    }

    public String getSuffix() {
        return getString(S3_SUFFIX_CONFIG);
    }

    public String getAccessKeyId() {
        return getString(AWS_ACCESS_KEY_ID_CONFIG);
    }

    public String getSecretAccessKey() {
        return getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value();
    }

    public String getSessionToken() {
        return getPassword(AWS_SESSION_TOKEN_CONFIG).value();
    }

    public long getPollIntervalMs() {
        return getLong(POLL_INTERVAL_MS_CONFIG);
    }

    public int getMaxObjectsPerPoll() {
        return getInt(MAX_OBJECTS_PER_POLL_CONFIG);
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    public String getOffsetStorageKey() {
        return getString(OFFSET_STORAGE_KEY_CONFIG);
    }

    public String getReadMode() {
        return getString(READ_MODE_CONFIG);
    }

    public String getFileFormat() {
        return getString(FILE_FORMAT_CONFIG);
    }

    public String getCsvDelimiter() {
        return getString(CSV_DELIMITER_CONFIG);
    }

    public boolean getCsvHeader() {
        return getBoolean(CSV_HEADER_CONFIG);
    }

    public boolean getJsonArrayMode() {
        return getBoolean(JSON_ARRAY_MODE_CONFIG);
    }

    public int getMaxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }

    public long getRetryBackoffMs() {
        return getLong(RETRY_BACKOFF_MS_CONFIG);
    }

    public int getConnectTimeoutMs() {
        return getInt(CONNECT_TIMEOUT_MS_CONFIG);
    }

    public int getSocketTimeoutMs() {
        return getInt(SOCKET_TIMEOUT_MS_CONFIG);
    }

    public boolean getIncludeMetadata() {
        return getBoolean(INCLUDE_METADATA_CONFIG);
    }

    public String getMetadataFieldPrefix() {
        return getString(METADATA_FIELD_PREFIX_CONFIG);
    }

    public boolean getFilterByLastModified() {
        return getBoolean(FILTER_BY_LAST_MODIFIED_CONFIG);
    }

    public String getLastModifiedAfter() {
        return getString(LAST_MODIFIED_AFTER_CONFIG);
    }

    public String getLastModifiedBefore() {
        return getString(LAST_MODIFIED_BEFORE_CONFIG);
    }

    public long getMinObjectSize() {
        return getLong(MIN_OBJECT_SIZE_CONFIG);
    }

    public long getMaxObjectSize() {
        return getLong(MAX_OBJECT_SIZE_CONFIG);
    }

    public int getTasksMax() {
        return getInt(TASKS_MAX_CONFIG);
    }

    public String getErrorHandling() {
        return getString(ERROR_HANDLING_CONFIG);
    }

    public String getDeadLetterTopic() {
        return getString(DEAD_LETTER_TOPIC_CONFIG);
    }

    public String getCompressionType() {
        return getString(COMPRESSION_TYPE_CONFIG);
    }

    public String getPartitionField() {
        return getString(PARTITION_FIELD_CONFIG);
    }

    public String getKeyField() {
        return getString(KEY_FIELD_CONFIG);
    }
    
    public String getEndpointUrl() {
        return getString(S3_ENDPOINT_URL_CONFIG);
    }
    
    public boolean getPathStyleAccess() {
        return getBoolean(S3_PATH_STYLE_ACCESS_CONFIG);
    }
}
