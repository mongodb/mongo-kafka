# Field Decryption + Client-Side Field Level Encryption (CS-FLE) Demo

This demo showcases two encryption features for legacy database-to-MongoDB migration scenarios.

## Overview

When migrating from legacy databases to MongoDB, you may encounter data that is encrypted with proprietary encryption methods. This demo shows how to:

1. **Decrypt** data encrypted with legacy encryption as it flows through the Kafka Connector
2. **Re-encrypt** the data using MongoDB's native Client-Side Field Level Encryption (CS-FLE) before storing it

## The Problem

Imagine you have employee records in a legacy database where sensitive fields (SSN, email) are encrypted:

```json
{
  "_id": 1,
  "name": "Alice Johnson",
  "ssn": "A+3BxZDKVFsBSRoYsD6dOg==",  // AES-encrypted
  "email": "tX571tgLeJhbf1kQHEQPwp2Vkldo3utNo3mUruMf8so=",  // AES-encrypted
  "dept": "Engineering"
}
```

You can't just copy this to MongoDB because:
- MongoDB doesn't know how to decrypt the legacy encryption
- You want to use MongoDB's own encryption system for better integration

## The Solution

This demo implements a **two-stage encryption pipeline**:

```
Legacy Database (AES-encrypted)
  |
  v
Kafka Topic (still AES-encrypted)
  |
  v
Sink Connector
  +-- FieldValueTransformPostProcessor -> Decrypts legacy AES
  +-- MongoClient with AutoEncryptionSettings -> Re-encrypts with CS-FLE
  |
  v
MongoDB (encrypted with CS-FLE)
```

The plaintext only exists briefly in memory during the transformation. The data is protected at every stage.

## Features Demonstrated

### 1. Pluggable Field Decryption

A flexible interface that allows you to provide custom decryption logic:

- **Interface**: `FieldValueTransformer`
- **Sample Implementation**: `SampleAesFieldValueTransformer` (AES-128-ECB decryption)
- **Configuration**: Specify which fields to decrypt and provide your decryption class

### 2. MongoDB Client-Side Field Level Encryption (CS-FLE)

MongoDB's native encryption that happens transparently in the driver:

- **Automatic**: No custom code needed, just configuration
- **Secure**: Only clients with the encryption key can read the data
- **Flexible**: Configure which fields to encrypt via JSON schema

## Architecture

### Components

- **MongoDB**: Stores the final encrypted data
- **Kafka**: Message broker (data flows through still encrypted with legacy encryption)
- **Kafka Connect**: Runs the MongoDB Sink Connector with our custom features
- **Demo Script**: Simulates legacy-encrypted data and verifies the pipeline

### Data Flow

1. **Source**: Demo creates employee records encrypted with AES-128-ECB (simulating legacy system)
2. **Kafka**: Records are published to a topic (still encrypted)
3. **Transformation**: Sink Connector decrypts using `SampleAesFieldValueTransformer`
4. **Re-encryption**: MongoDB driver encrypts with CS-FLE before writing
5. **Storage**: Data is stored in MongoDB, encrypted with CS-FLE

## Requirements

### System Requirements

- **Docker** and **Docker Compose**
- **Java 17+** and **Gradle** (for building the connector)
- **x86_64 architecture** (required for CS-FLE native libraries)

### Why x86_64?

The `mongodb-crypt` library includes native code (C++) that interfaces with Java via JNA (Java Native Access). The native library requires glibc, which is not compatible with:
- Alpine Linux (uses musl libc)
- ARM64 architecture in Docker (library compatibility issues)

**This demo will work on:**
- [YES] x86_64 Linux (Intel/AMD)
- [YES] x86_64 macOS with Docker Desktop
- [YES] Windows with Docker Desktop (WSL2 backend)

**This demo will NOT work on:**
- [NO] ARM64 (Apple Silicon M1/M2/M3) without x86_64 emulation
- [NO] Alpine Linux-based containers

### Workaround for ARM64

If you're on Apple Silicon, you can run the demo with x86_64 emulation:

```bash
export DOCKER_DEFAULT_PLATFORM=linux/amd64
./run-demo.sh
```

Note: This will be slower due to emulation.

## Running the Demo

### Quick Start

```bash
cd demos/field-decryption-and-csfle
./run-demo.sh
```

The script will:
1. Check your architecture
2. Build the MongoDB Kafka Connector JAR (if needed)
3. Start Docker containers (MongoDB, Zookeeper, Kafka, Kafka Connect)
4. Wait for services to be ready
5. Run the encryption demo
6. Show you the results

### Options

```bash
./run-demo.sh [--rebuild-jar] [--clean]
```

- `--rebuild-jar`: Force rebuild of the connector JAR from source
- `--clean`: Stop and remove all containers before starting (fresh start)

### Manual Steps

If you prefer to run steps manually:

1. **Build the connector:**
   ```bash
   cd ../../  # Go to project root
   ./gradlew clean allJar -x test -x spotlessJava
   cp build/libs/mongo-kafka-connect-*-all.jar demos/field-decryption-and-csfle/connector-jar/mongo-kafka-connect.jar
   ```

2. **Start containers:**
   ```bash
   cd demos/field-decryption-and-csfle
   docker compose up --build -d
   ```

3. **Wait for Kafka Connect:**
   ```bash
   # Wait until this returns successfully
   curl http://localhost:8083/connectors
   ```

4. **Run the demo:**
   ```bash
   ./demo-encryption-with-csfle.sh
   ```

## What the Demo Shows

The demo will output:

1. **Source Data** (Legacy AES encryption):
   ```
   ssn: "A+3BxZDKVFsBSRoYsD6dOg=="
   ```

2. **Sink Data** (MongoDB CS-FLE encryption):
   ```
   ssn: <Binary subtype 6, encrypted>
   ```

3. **Verification**: Confirms the connector is running and data was encrypted

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

### CS-FLE Configuration

```properties
# Enable CS-FLE
csfle.enabled=true

# Key vault location (database.collection)
csfle.key.vault.namespace=encryption.__keyVault

# Base64-encoded 96-byte master key (use secure KMS in production)
csfle.local.master.key=<base64-key>

# JSON schema defining which fields to encrypt
csfle.schema.map={"db.collection": {"bsonType": "object", "properties": {...}}}
```

## Implementing Your Own Decryption

To use your own decryption logic instead of the sample AES transformer:

1. **Create a class implementing `FieldValueTransformer`:**

```java
package com.example;

import com.mongodb.kafka.connect.sink.processor.field.transform.FieldValueTransformer;
import org.bson.BsonValue;
import org.bson.BsonString;
import java.util.Map;

public class MyCustomDecryptor implements FieldValueTransformer {

    private String decryptionKey;

    @Override
    public void init(Map<String, String> configs) {
        // Read your custom configuration properties
        // Property pattern: field.value.transformer.<your-property-name>
        // "custom.key" is a custom property name you define - it can be anything you want
        // (e.g., "mycompany.secret", "legacy.key", etc.)
        this.decryptionKey = configs.get("field.value.transformer.custom.key");
        if (decryptionKey == null || decryptionKey.isEmpty()) {
            throw new IllegalArgumentException("field.value.transformer.custom.key must be provided");
        }
    }

    @Override
    public BsonValue transform(String fieldName, BsonValue value) {
        if (!value.isString()) {
            return value;  // Skip non-string values
        }

        String encrypted = value.asString().getValue();
        // Your custom decryption logic here
        String decrypted = decryptLegacyEncryption(encrypted, decryptionKey);
        return new BsonString(decrypted);
    }

    private String decryptLegacyEncryption(String encrypted, String key) {
        // Implement your legacy system's decryption algorithm
        // ...
        return encrypted; // placeholder
    }
}
```

2. **Package it in a JAR** and place it in the Kafka Connect plugin directory

3. **Configure the connector:**
```properties
field.value.transformer=com.example.MyCustomDecryptor
field.value.transformer.fields=ssn,email
# "custom.key" is your custom property name - must match what you use in init() method
field.value.transformer.custom.key=your-decryption-key
```

## Troubleshooting

### Kafka Connect crashes with SIGSEGV

This means the native `libmongocrypt.so` library failed to load. Common causes:
- Running on ARM64 without x86_64 emulation
- Using Alpine Linux (needs glibc, not musl)

**Solution**: Use x86_64 architecture or enable emulation.

### Connector fails with "NoClassDefFoundError: MongoCryptOptions"

The `mongodb-crypt` JAR is not in the connector plugin directory.

**Solution**: Rebuild with `./run-demo.sh --rebuild-jar`

### Demo times out waiting for documents

Check the connector status:
```bash
curl http://localhost:8083/connectors/csfle-sink/status | jq .
```

Check logs:
```bash
docker logs kafka-connect
```

## Cleanup

Stop and remove all containers:
```bash
docker compose down -v
```

## Implementation Details

### Field Decryption Architecture

**Interface**: `FieldValueTransformer`
- Location: `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformer.java`
- Methods:
  - `init(Map<String, String> configs)` - Initialize with configuration
  - `BsonValue transform(String fieldName, BsonValue value)` - Transform/decrypt a single value

**PostProcessor**: `FieldValueTransformPostProcessor`
- Location: `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformPostProcessor.java`
- Recursively walks document structure (including nested fields and arrays)
- Applies transformation only to configured fields
- Automatically added to PostProcessor chain when transformer is configured

**Sample Implementation**: `SampleAesFieldValueTransformer`
- Location: `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/SampleAesFieldValueTransformer.java`
- Demonstrates AES-128-ECB decryption
- Serves as a reference for implementing custom decryption logic

**Configuration Properties** (in `MongoSinkTopicConfig`):
- `field.value.transformer` - Fully qualified class name of transformer
- `field.value.transformer.fields` - Comma-separated list of fields to transform
- `field.value.transformer.fail.on.error` - Whether to fail on transformation errors (default: true)
- Custom properties: Any property starting with `field.value.transformer.*` is passed to the transformer

### CS-FLE Architecture

**MongoClient Enhancement** (in `MongoSinkTask`):
- `buildAutoEncryptionSettings()` - Constructs `AutoEncryptionSettings` from configuration
- `parseSchemaMap()` - Parses JSON schema map for field encryption rules
- Modified `createMongoClient()` to attach `AutoEncryptionSettings` when CS-FLE is enabled

**Configuration Properties** (in `MongoSinkConfig`):
- `csfle.enabled` - Enable/disable CS-FLE (default: false)
- `csfle.key.vault.namespace` - Key vault location (format: `database.collection`)
- `csfle.local.master.key` - Base64-encoded 96-byte master key for local KMS
- `csfle.schema.map` - JSON schema defining which fields to encrypt

**Dependency**:
- `mongodb-crypt:1.11.0` (compatible with MongoDB Java Driver 4.7.x)
- Included in connector JAR for CS-FLE functionality

## Known Limitations

1. **Platform**: CS-FLE native library works best on x86_64 with glibc-based Linux
2. **ARM64**: Requires x86_64 emulation (slower) or may not work in some environments
3. **Alpine Linux**: Not compatible due to musl libc vs glibc requirement
4. **Schema Map**: Must be valid JSON; complex schemas can be error-prone

## Production Considerations

1. **Key Management**: Use a proper KMS (AWS KMS, Azure Key Vault, GCP KMS) instead of local keys
2. **Key Rotation**: Implement a key rotation strategy
3. **Performance**: CS-FLE adds encryption overhead; benchmark with your workload
4. **Error Handling**: Set `field.value.transformer.fail.on.error` based on your requirements
5. **Monitoring**: Monitor transformation errors and connector performance
6. **Security**: Ensure encryption keys are never logged or exposed
7. **Testing**: Test with your actual encryption implementation before production deployment

## Learn More

- [MongoDB Client-Side Field Level Encryption](https://www.mongodb.com/docs/manual/core/csfle/)
- [MongoDB Kafka Connector Documentation](https://www.mongodb.com/docs/kafka-connector/current/)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
