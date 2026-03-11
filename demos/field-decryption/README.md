# Field Decryption Demo

This demo showcases the **field-level decryption** feature of the MongoDB Kafka Sink Connector for migrating encrypted data from legacy systems to MongoDB.

## Overview

When migrating from legacy databases (Oracle, SQL Server, etc.) to MongoDB via Kafka, you may encounter data encrypted with proprietary encryption methods. This demo shows how to **decrypt** that data as it flows through the Kafka Connector before storing it in MongoDB.

## The Problem

Imagine you have employee records in a legacy database where sensitive fields (SSN, email) are encrypted with AES:

```json
{
  "_id": 1,
  "name": "Alice Johnson",
  "ssn": "kX9vZ2pL3mN5qR8tY1wE4g==",     // AES-encrypted
  "email": "pQ2sT6uV9xA3bC7dF1gH5j==",  // AES-encrypted
  "dept": "Engineering"
}
```

You can't just copy this to MongoDB because the application expects plaintext data.

## The Solution

This demo implements a **decryption pipeline**:

```
Legacy Database (AES-encrypted)
  |
  v
Kafka Topic (still AES-encrypted)
  |
  v
Sink Connector
  +-- FieldValueTransformPostProcessor -> Decrypts to plaintext
  |
  v
MongoDB (plaintext)
```

The encrypted data flows through Kafka and is decrypted only when written to MongoDB.

## Features Demonstrated

### Pluggable Field Decryption

A flexible interface that allows you to provide custom decryption logic:

- **Interface**: `FieldValueTransformer` - implement this with your decryption algorithm
- **Sample Implementation**: `SampleAesFieldValueTransformer` - decrypts AES-128-ECB encrypted, Base64-encoded values
- **Configuration**: Specify which fields to decrypt and pass custom parameters (e.g., encryption keys)

## Architecture

The demo uses Docker Compose to run:

- **MongoDB**: Stores the final decrypted data
- **Kafka**: Message broker (data flows through still encrypted)
- **Kafka Connect**: Runs the MongoDB Sink Connector with field decryption
- **Demo Script**: Simulates encrypted data and verifies the pipeline

### Data Flow

1. **Source**: Demo creates employee records encrypted with AES-128-ECB (simulating legacy system)
2. **Kafka**: Records are published to a topic (still encrypted)
3. **Transformation**: Sink Connector decrypts using `SampleAesFieldValueTransformer`
4. **Storage**: Data is stored in MongoDB as plaintext

## Requirements

- Docker and Docker Compose
- Java 17+ and Gradle (for building the connector)
- Bash shell

## Quick Start

```bash
cd demos/field-decryption
./run-demo.sh
```

This will:
1. Build the MongoDB Kafka Connector JAR (if not already built)
2. Start all Docker containers
3. Run the decryption demo
4. Show you the encrypted source data and decrypted sink data

### Options

```bash
./run-demo.sh --rebuild-jar  # Force rebuild of connector JAR
./run-demo.sh --clean        # Clean up containers before starting
```

## What the Demo Does

1. **Creates encrypted data**: Inserts employee records with AES-encrypted SSN and email fields into `demo.encrypted_source`
2. **Publishes to Kafka**: Source connector reads from MongoDB and publishes to Kafka topic `demo.encrypted_source`
3. **Decrypts and stores**: Sink connector reads from Kafka, decrypts the fields, and writes plaintext to `demo.decrypted_sink`
4. **Verification**: Shows both encrypted source and decrypted sink data side-by-side

## Configuration

### Field Decryption Configuration

```properties
# Your custom decryption class (must implement FieldValueTransformer)
field.value.transformer=com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer

# Comma-separated list of fields to decrypt
field.value.transformer.fields=ssn,email

# Whether to fail if decryption fails (default: true)
field.value.transformer.fail.on.error=true

# Custom properties for your transformer (example: AES key)
field.value.transformer.aes.key=mySecretKey12345
```

## Customer Usage

To use this feature with your own encryption:

1. **Implement `FieldValueTransformer` interface**:
   ```java
   public class MyDecryptor implements FieldValueTransformer {
     private String decryptionKey;

     @Override
     public void init(Map<String, String> configs) {
       this.decryptionKey = configs.get("field.value.transformer.my.key");
     }

     @Override
     public BsonValue transform(String fieldName, BsonValue value) {
       // Your decryption logic here
       return decryptedValue;
     }
   }
   ```

2. **Package as JAR** and place in Kafka Connect plugin directory

3. **Configure connector**:
   ```properties
   field.value.transformer=com.customer.MyDecryptor
   field.value.transformer.fields=ssn,email,credit_card
   field.value.transformer.my.key=your-decryption-key
   ```

## Cleanup

```bash
cd demos/field-decryption
docker compose down -v
```

## Related Demos

- **[field-decryption-and-csfle](../field-decryption-and-csfle/)**: Shows decryption + re-encryption with MongoDB Client-Side Field Level Encryption (CS-FLE)

## Troubleshooting

### Connector fails to start
Check the logs:
```bash
docker logs kafka-connect
```

### Data not appearing in sink collection
1. Check connector status: `curl http://localhost:8083/connectors/dec-sink/status`
2. Check Kafka topic has messages: `docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic demo.encrypted_source --from-beginning --max-messages 1`

### Decryption errors
- Verify the AES key matches the encryption key
- Check that encrypted fields are Base64-encoded strings
- Review connector logs for detailed error messages
