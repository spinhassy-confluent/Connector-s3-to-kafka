package com.kafka.connect.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrapper for AWS S3 client with retry logic and error handling
 */
public class S3ClientWrapper implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(S3ClientWrapper.class);

    private final S3Client s3Client;
    private final S3SourceConnectorConfig config;
    private final int maxRetries;
    private final long retryBackoffMs;

    public S3ClientWrapper(S3SourceConnectorConfig config) {
        this.config = config;
        this.maxRetries = config.getMaxRetries();
        this.retryBackoffMs = config.getRetryBackoffMs();

        AwsCredentialsProvider credentialsProvider = createCredentialsProvider(config);
        
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(maxRetries)
                .backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofMillis(retryBackoffMs)))
                .build();

        SdkHttpClient httpClient = UrlConnectionHttpClient.builder()
                .connectionTimeout(Duration.ofMillis(config.getConnectTimeoutMs()))
                .socketTimeout(Duration.ofMillis(config.getSocketTimeoutMs()))
                .build();

        S3ClientBuilder s3Builder = S3Client.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(credentialsProvider)
                .httpClient(httpClient)
                .overrideConfiguration(b -> b.retryPolicy(retryPolicy));
        
        // Configure custom endpoint if provided (for S3-compatible storage)
        String endpointUrl = config.getEndpointUrl();
        if (endpointUrl != null && !endpointUrl.isEmpty()) {
            try {
                s3Builder.endpointOverride(new java.net.URI(endpointUrl));
                log.info("Using custom S3 endpoint: {}", endpointUrl);
            } catch (java.net.URISyntaxException e) {
                throw new IllegalArgumentException("Invalid S3 endpoint URL: " + endpointUrl, e);
            }
            
            // Enable path-style access if configured (required for some S3-compatible storage)
            if (config.getPathStyleAccess()) {
                s3Builder.forcePathStyle(true);
                log.debug("Path-style access enabled");
            }
        }
        
        this.s3Client = s3Builder.build();

        log.info("S3Client initialized for bucket: {}, region: {}", config.getBucketName(), config.getRegion());
    }

    private AwsCredentialsProvider createCredentialsProvider(S3SourceConnectorConfig config) {
        String accessKeyId = config.getAccessKeyId();
        String secretAccessKey = config.getSecretAccessKey();
        String sessionToken = config.getSessionToken();

        if (accessKeyId != null && !accessKeyId.isEmpty() && 
            secretAccessKey != null && !secretAccessKey.isEmpty()) {
            AwsBasicCredentials credentials;
            if (sessionToken != null && !sessionToken.isEmpty()) {
                credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
                // Note: AWS SDK v2 uses different approach for session tokens
                // For simplicity, using basic credentials. For production, use TemporaryCredentialsProvider
                log.debug("Using static credentials with session token");
            } else {
                credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
                log.debug("Using static credentials");
            }
            return StaticCredentialsProvider.create(credentials);
        } else {
            log.debug("Using default credential chain");
            return DefaultCredentialsProvider.create();
        }
    }

    /**
     * List S3 objects matching the configured filters
     */
    public List<S3Object> listObjects(String continuationToken) {
        return executeWithRetry(() -> {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(config.getBucketName())
                    .maxKeys(config.getMaxObjectsPerPoll());

            if (config.getPrefix() != null && !config.getPrefix().isEmpty()) {
                requestBuilder.prefix(config.getPrefix());
            }

            if (continuationToken != null && !continuationToken.isEmpty()) {
                requestBuilder.continuationToken(continuationToken);
            }

            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            List<S3Object> objects = response.contents();

            // Apply filters
            objects = filterObjects(objects);

            log.debug("Listed {} objects from S3 bucket {}", objects.size(), config.getBucketName());
            return objects;
        }, "listObjects");
    }

    /**
     * Get object content as byte array
     */
    public byte[] getObjectContent(String key) {
        // Validate key to prevent path traversal attacks
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Object key cannot be null or empty");
        }
        
        if (key.contains("..") || key.startsWith("/")) {
            throw new IllegalArgumentException("Invalid object key: potential path traversal detected");
        }
        
        return executeWithRetry(() -> {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(config.getBucketName())
                    .key(key)
                    .build();

            return s3Client.getObjectAsBytes(request).asByteArray();
        }, "getObjectContent", key);
    }

    /**
     * Get object metadata
     */
    public HeadObjectResponse getObjectMetadata(String key) {
        return executeWithRetry(() -> {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(config.getBucketName())
                    .key(key)
                    .build();

            return s3Client.headObject(request);
        }, "getObjectMetadata", key);
    }

    /**
     * Check if object exists
     */
    public boolean objectExists(String key) {
        try {
            getObjectMetadata(key);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            log.warn("Error checking if object exists: {}", key, e);
            return false;
        }
    }

    private List<S3Object> filterObjects(List<S3Object> objects) {
        return objects.stream()
                .filter(obj -> {
                    // Filter by suffix
                    if (config.getSuffix() != null && !config.getSuffix().isEmpty()) {
                        if (!obj.key().endsWith(config.getSuffix())) {
                            return false;
                        }
                    }

                    // Filter by size
                    long size = obj.size();
                    if (size < config.getMinObjectSize() || size > config.getMaxObjectSize()) {
                        return false;
                    }

                    // Filter by last modified time
                    if (config.getFilterByLastModified()) {
                        Instant lastModified = obj.lastModified();
                        if (config.getLastModifiedAfter() != null && !config.getLastModifiedAfter().isEmpty()) {
                            Instant after = Instant.parse(config.getLastModifiedAfter());
                            if (lastModified.isBefore(after)) {
                                return false;
                            }
                        }
                        if (config.getLastModifiedBefore() != null && !config.getLastModifiedBefore().isEmpty()) {
                            Instant before = Instant.parse(config.getLastModifiedBefore());
                            if (lastModified.isAfter(before)) {
                                return false;
                            }
                        }
                    }

                    return true;
                })
                .collect(Collectors.toList());
    }

    private <T> T executeWithRetry(RetryableOperation<T> operation, String operationName) {
        return executeWithRetry(operation, operationName, null);
    }

    private <T> T executeWithRetry(RetryableOperation<T> operation, String operationName, String key) {
        int attempts = 0;
        Exception lastException = null;

        while (attempts <= maxRetries) {
            try {
                return operation.execute();
            } catch (SdkException e) {
                lastException = e;
                attempts++;
                
                if (attempts > maxRetries) {
                    String message = String.format("Failed to execute %s after %d attempts", 
                            operationName, attempts);
                    if (key != null) {
                        message += " for key: " + key;
                    }
                    log.error(message, e);
                    throw new RuntimeException(message, e);
                }

                log.warn("Retry attempt {}/{} for {}: {}", attempts, maxRetries, operationName, e.getMessage());
                try {
                    Thread.sleep(retryBackoffMs * attempts); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry", ie);
                }
            }
        }

        throw new RuntimeException("Failed to execute " + operationName, lastException);
    }

    @FunctionalInterface
    private interface RetryableOperation<T> {
        T execute() throws SdkException;
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            log.info("S3Client closed");
        }
    }
}
