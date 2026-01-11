package com.kafka.connect.s3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Parses different file formats from S3 objects
 */
public class FileFormatParser {
    private static final Logger log = LoggerFactory.getLogger(FileFormatParser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final S3SourceConnectorConfig config;

    public FileFormatParser(S3SourceConnectorConfig config) {
        this.config = config;
    }

    /**
     * Parse file content based on configured format
     */
    public List<Map<String, Object>> parse(byte[] content, String objectKey) {
        String format = config.getFileFormat().toLowerCase();

        switch (format) {
            case "json":
                return parseJson(content, objectKey);
            case "csv":
                return parseCsv(content, objectKey);
            case "text":
                return parseText(content, objectKey);
            case "avro":
                return parseAvro(content, objectKey);
            case "binary":
                return parseBinary(content, objectKey);
            default:
                throw new IllegalArgumentException("Unsupported file format: " + format);
        }
    }

    private List<Map<String, Object>> parseJson(byte[] content, String objectKey) {
        List<Map<String, Object>> records = new ArrayList<>();
        String contentStr = new String(content, StandardCharsets.UTF_8);

        try {
            if (config.getJsonArrayMode()) {
                // Parse as JSON array
                JsonNode rootNode = objectMapper.readTree(contentStr);
                if (rootNode.isArray()) {
                    for (JsonNode node : rootNode) {
                        Map<String, Object> record = objectMapper.convertValue(node, 
                            new TypeReference<Map<String, Object>>() {});
                        records.add(record);
                    }
                } else {
                    // Single object
                    Map<String, Object> record = objectMapper.convertValue(rootNode, 
                        new TypeReference<Map<String, Object>>() {});
                    records.add(record);
                }
            } else {
                // Each line is a JSON object (JSONL format)
                String[] lines = contentStr.split("\n");
                for (String line : lines) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        try {
                            Map<String, Object> record = objectMapper.readValue(line, 
                                new TypeReference<Map<String, Object>>() {});
                            records.add(record);
                        } catch (Exception e) {
                            log.warn("Failed to parse JSON line in {}: {}", objectKey, line, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse JSON from {}", objectKey, e);
            throw new RuntimeException("JSON parsing failed", e);
        }

        return records;
    }

    private List<Map<String, Object>> parseCsv(byte[] content, String objectKey) {
        List<Map<String, Object>> records = new ArrayList<>();
        String delimiter = config.getCsvDelimiter();
        boolean hasHeader = config.getCsvHeader();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8))) {
            
            String[] headers = null;
            String line;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                String[] values = parseCsvLine(line, delimiter);

                if (hasHeader && headers == null) {
                    headers = values;
                    continue;
                }

                Map<String, Object> record = new HashMap<>();
                if (headers != null) {
                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        record.put(headers[i].trim(), values[i]);
                    }
                } else {
                    // No header, use column indices
                    for (int i = 0; i < values.length; i++) {
                        record.put("column_" + i, values[i]);
                    }
                }
                records.add(record);
            }
        } catch (Exception e) {
            log.error("Failed to parse CSV from {}", objectKey, e);
            throw new RuntimeException("CSV parsing failed", e);
        }

        return records;
    }
    
    /**
     * Parse a CSV line handling quoted fields that may contain delimiters
     */
    private String[] parseCsvLine(String line, String delimiter) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // Handle escaped quotes
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++; // Skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (!inQuotes && String.valueOf(c).equals(delimiter)) {
                result.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        // Add the last field
        result.add(current.toString().trim());
        
        return result.toArray(new String[0]);
    }

    private List<Map<String, Object>> parseText(byte[] content, String objectKey) {
        List<Map<String, Object>> records = new ArrayList<>();
        String contentStr = new String(content, StandardCharsets.UTF_8);
        String[] lines = contentStr.split("\n");

        for (String line : lines) {
            line = line.trim();
            if (!line.isEmpty()) {
                Map<String, Object> record = new HashMap<>();
                record.put("line", line);
                records.add(record);
            }
        }

        return records;
    }

    private List<Map<String, Object>> parseAvro(byte[] content, String objectKey) {
        // Avro parsing would require additional dependencies
        // For now, return as binary
        log.warn("Avro parsing not fully implemented, treating as binary");
        return parseBinary(content, objectKey);
    }

    private List<Map<String, Object>> parseBinary(byte[] content, String objectKey) {
        Map<String, Object> record = new HashMap<>();
        record.put("data", Base64.getEncoder().encodeToString(content));
        record.put("size", content.length);
        return Collections.singletonList(record);
    }
}
