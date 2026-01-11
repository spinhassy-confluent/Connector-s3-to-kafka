package com.kafka.connect.s3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main connector class for S3 Source Connector
 * Manages task configuration and lifecycle
 */
public class S3SourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(S3SourceConnector.class);

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting S3 Source Connector");
        configProps = props;
        
        // Validate configuration
        try {
            new S3SourceConnectorConfig(props);
        } catch (Exception e) {
            log.error("Invalid configuration: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start connector due to invalid configuration", e);
        }
        
        log.info("S3 Source Connector started successfully");
    }

    @Override
    public void stop() {
        log.info("Stopping S3 Source Connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return S3SourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        S3SourceConnectorConfig config = new S3SourceConnectorConfig(configProps);
        int tasksMax = config.getTasksMax();
        int actualTasks = Math.min(maxTasks, tasksMax);
        
        log.info("Creating {} task configurations", actualTasks);
        
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        
        if (actualTasks == 1) {
            // Single task gets all configuration
            taskConfigs.add(new HashMap<>(configProps));
        } else {
            // Multiple tasks - each task can handle a subset of objects
            // For simplicity, all tasks get the same config and filter by task ID
            for (int i = 0; i < actualTasks; i++) {
                Map<String, String> taskConfig = new HashMap<>(configProps);
                taskConfig.put("task.id", String.valueOf(i));
                taskConfigs.add(taskConfig);
            }
        }
        
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        return S3SourceConnectorConfig.config();
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
