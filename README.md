# S3 Source Connector for Apache Kafka

A robust, configurable Kafka Connect source connector that reads data from Amazon S3 and publishes it to Kafka topics.

## Features

- **Multiple File Formats**: Supports JSON, CSV, Text, Avro, and Binary formats
- **Flexible Filtering**: Filter objects by prefix, suffix, size, and last modified time
- **Incremental Processing**: Track processed objects to avoid reprocessing
- **Error Handling**: Configurable error handling with dead letter queue support
- **Retry Logic**: Automatic retry with exponential backoff for transient failures
- **Metadata Support**: Optionally include S3 object metadata in Kafka records
- **Configurable Batching**: Control batch size and polling intervals
- **Multi-Task Support**: Scale horizontally with multiple tasks

## Requirements

- Java 11 or higher
- Apache Kafka 2.8.0 or higher
- Kafka Connect framework
- AWS S3 bucket with appropriate IAM permissions

## Building

```bash
mvn clean package
```

This will create a JAR file in the `target` directory with all dependencies included.

## Installation

1. Copy the JAR file to your Kafka Connect plugin path:
   ```bash
   cp target/s3-source-connector-1.0.0.jar /path/to/kafka/connect/plugins/
   ```

2. Restart your Kafka Connect worker

## Configuration

### Mandatory Configuration

| Property | Type | Description |
|----------|------|-------------|
| `s3.bucket.name` | string | Name of the S3 bucket to read from |
| `topic` | string | Kafka topic to write data to |

### Optional Configuration

#### AWS Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `s3.region` | string | `us-east-1` | AWS region where the S3 bucket is located |
| `aws.access.key.id` | string | (empty) | AWS access key ID. If not provided, uses default credential chain |
| `aws.secret.access.key` | password | (empty) | AWS secret access key. If not provided, uses default credential chain |
| `aws.session.token` | password | (empty) | AWS session token for temporary credentials |

#### S3 Filtering

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `s3.prefix` | string | (empty) | Prefix to filter S3 objects (e.g., `folder/subfolder/`) |
| `s3.suffix` | string | (empty) | Suffix to filter S3 objects (e.g., `.json`, `.csv`) |
| `filter.by.last.modified` | boolean | `false` | Filter objects by last modified time |
| `last.modified.after` | string | (empty) | Only process objects modified after this timestamp (ISO 8601 format) |
| `last.modified.before` | string | (empty) | Only process objects modified before this timestamp (ISO 8601 format) |
| `min.object.size` | long | `0` | Minimum object size in bytes to process |
| `max.object.size` | long | `Long.MAX_VALUE` | Maximum object size in bytes to process |

#### Polling Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `poll.interval.ms` | long | `60000` | Interval in milliseconds between polling S3 for new objects |
| `max.objects.per.poll` | int | `100` | Maximum number of S3 objects to process in a single poll |
| `batch.size` | int | `1000` | Number of records to batch before sending to Kafka |

#### Offset and Read Mode

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `offset.storage.key` | string | `s3-source-connector-offset` | Key used to store offset information in Kafka Connect |
| `read.mode` | string | `full` | Read mode: `full` (read entire file) or `incremental` (only new/changed files) |

#### File Format Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `file.format` | string | `json` | File format: `json`, `csv`, `text`, `avro`, or `binary` |
| `csv.delimiter` | string | `,` | Delimiter for CSV files |
| `csv.header` | boolean | `true` | Whether CSV files have a header row |
| `json.array.mode` | boolean | `false` | If true, treats each file as a JSON array and splits into individual records |

#### Error Handling and Retry

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `max.retries` | int | `3` | Maximum number of retries for failed operations |
| `retry.backoff.ms` | long | `1000` | Backoff time in milliseconds between retries |
| `error.handling` | string | `fail` | Error handling strategy: `fail` (stop on error) or `skip` (skip failed objects) |
| `dead.letter.topic` | string | (empty) | Topic to send failed records to (required if `error.handling` is `skip`) |

#### Timeout Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `connect.timeout.ms` | int | `10000` | Connection timeout in milliseconds |
| `socket.timeout.ms` | int | `50000` | Socket timeout in milliseconds |

#### Metadata Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `include.metadata` | boolean | `true` | Include S3 object metadata (key, size, lastModified) in Kafka records |
| `metadata.field.prefix` | string | `__s3_` | Prefix for metadata fields in Kafka records |

#### Kafka Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `tasks.max` | int | `1` | Maximum number of tasks for this connector |
| `compression.type` | string | `none` | Compression type: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `partition.field` | string | (empty) | Field name to use for partitioning Kafka records |
| `key.field` | string | (empty) | Field name to use as Kafka record key |

## Example Configuration

### Basic Configuration

```json
{
  "name": "s3-source-connector",
  "config": {
    "connector.class": "com.kafka.connect.s3.S3SourceConnector",
    "s3.bucket.name": "my-bucket",
    "topic": "s3-data",
    "s3.region": "us-west-2",
    "file.format": "json",
    "tasks.max": "1"
  }
}
```

### Advanced Configuration with Filtering

```json
{
  "name": "s3-source-connector-advanced",
  "config": {
    "connector.class": "com.kafka.connect.s3.S3SourceConnector",
    "s3.bucket.name": "my-bucket",
    "topic": "s3-data",
    "s3.region": "us-west-2",
    "s3.prefix": "data/",
    "s3.suffix": ".json",
    "file.format": "json",
    "json.array.mode": "true",
    "read.mode": "incremental",
    "poll.interval.ms": "30000",
    "max.objects.per.poll": "50",
    "batch.size": "500",
    "include.metadata": "true",
    "metadata.field.prefix": "__s3_",
    "filter.by.last.modified": "true",
    "last.modified.after": "2024-01-01T00:00:00Z",
    "min.object.size": "1024",
    "max.object.size": "10485760",
    "error.handling": "skip",
    "dead.letter.topic": "s3-dlq",
    "max.retries": "5",
    "retry.backoff.ms": "2000",
    "tasks.max": "2"
  }
}
```

### CSV Configuration

```json
{
  "name": "s3-csv-connector",
  "config": {
    "connector.class": "com.kafka.connect.s3.S3SourceConnector",
    "s3.bucket.name": "my-bucket",
    "topic": "csv-data",
    "s3.region": "us-west-2",
    "s3.suffix": ".csv",
    "file.format": "csv",
    "csv.delimiter": ",",
    "csv.header": "true",
    "tasks.max": "1"
  }
}
```

## Usage

### Creating a Connector via REST API

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

### Checking Connector Status

```bash
curl http://localhost:8083/connectors/s3-source-connector/status
```

### Deleting a Connector

```bash
curl -X DELETE http://localhost:8083/connectors/s3-source-connector
```

## File Format Details

### JSON Format

- **JSONL Mode (default)**: Each line is treated as a separate JSON object
- **Array Mode**: File is treated as a JSON array, each element becomes a record

### CSV Format

- Supports custom delimiters
- Header row support (first row used as field names)
- Without header, columns are named `column_0`, `column_1`, etc.

### Text Format

- Each line becomes a separate record
- Record contains a single `line` field with the text content

### Binary Format

- Entire file content is Base64 encoded
- Record contains `data` (Base64 string) and `size` fields

## Offset Management

The connector tracks processed objects using Kafka Connect's offset storage:

- **Full Mode**: Processes all objects matching filters on each poll
- **Incremental Mode**: Only processes new or modified objects

Offset information includes:
- Object key
- Last modified timestamp
- Processing timestamp

## Error Handling

### Fail Mode (default)

When an error occurs, the connector stops and reports the error. Requires manual intervention.

### Skip Mode

When an error occurs, the connector:
1. Logs the error
2. Skips the problematic object
3. Optionally sends error information to a dead letter topic
4. Continues processing other objects

## Monitoring

Monitor the connector using:

- Kafka Connect REST API metrics
- Connector status endpoint
- Kafka consumer lag metrics
- Application logs

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify AWS credentials and IAM permissions
2. **Timeout Errors**: Increase `connect.timeout.ms` and `socket.timeout.ms`
3. **Memory Issues**: Reduce `batch.size` and `max.objects.per.poll`
4. **Processing Delays**: Adjust `poll.interval.ms` based on your needs

### Logging

Enable debug logging by setting the log level:

```properties
log4j.logger.com.kafka.connect.s3=DEBUG
```

## Security

- Use IAM roles when possible instead of access keys
- Store sensitive credentials in a secure credential store
- Use VPC endpoints for S3 access when in AWS
- Enable S3 bucket encryption

## License

This connector is provided as-is for use with Apache Kafka Connect.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Tests are included for new features
- Documentation is updated

## Support

For issues and questions, please open an issue in the project repository.
