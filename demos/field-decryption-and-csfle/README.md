# Field Decryption + Client-Side Field Level Encryption (CS-FLE) Demo

This demo showcases the two encryption features implemented in KAFKA-470 for Oracle-to-MongoDB migration scenarios.

## Overview

When migrating from Oracle (or other databases) to MongoDB, you may encounter data that is encrypted with proprietary encryption methods. This demo shows how to:

1. **Decrypt** data encrypted with Oracle/Hibernate encryption as it flows through the Kafka Connector
2. **Re-encrypt** the data using MongoDB's native Client-Side Field Level Encryption (CS-FLE) before storing it

## The Problem

Imagine you have employee records in Oracle where sensitive fields (SSN, email) are encrypted:

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
- MongoDB doesn't know how to decrypt Oracle's encryption
- You want to use MongoDB's own encryption system for better integration

## The Solution

This demo implements a **two-stage encryption pipeline**:

```
Oracle Database (AES-encrypted)
  ↓
Kafka Topic (still AES-encrypted)
  ↓
Sink Connector
  ├─ FieldValueTransformPostProcessor → Decrypts Oracle AES
  └─ MongoClient with AutoEncryptionSettings → Re-encrypts with CS-FLE
  ↓
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
- **Kafka**: Message broker (data flows through still encrypted with Oracle encryption)
- **Kafka Connect**: Runs the MongoDB Sink Connector with our custom features
- **Demo Script**: Simulates Oracle-encrypted data and verifies the pipeline

### Data Flow

1. **Source**: Demo creates employee records encrypted with AES-128-ECB (simulating Oracle)
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
- ✅ x86_64 Linux (Intel/AMD)
- ✅ x86_64 macOS with Docker Desktop
- ✅ Windows with Docker Desktop (WSL2 backend)

**This demo will NOT work on:**
- ❌ ARM64 (Apple Silicon M1/M2/M3) without x86_64 emulation
- ❌ Alpine Linux-based containers

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

1. **Source Data** (Oracle AES encryption):
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
import java.util.Map;

public class MyOracleDecryptor implements FieldValueTransformer {
    
    private String decryptionKey;
    
    @Override
    public void configure(Map<String, ?> configs) {
        // Read configuration
        this.decryptionKey = (String) configs.get("field.value.transformer.oracle.key");
    }
    
    @Override
    public Object transform(Object value) {
        if (!(value instanceof String)) {
            return value;  // Skip non-string values
        }
        
        String encrypted = (String) value;
        // Your Oracle decryption logic here
        return decryptOracleEncryption(encrypted, decryptionKey);
    }
    
    private String decryptOracleEncryption(String encrypted, String key) {
        // Implement your Oracle/Hibernate decryption algorithm
        // ...
    }
}
```

2. **Package it in a JAR** and place it in the Kafka Connect plugin directory

3. **Configure the connector:**
```properties
field.value.transformer=com.example.MyOracleDecryptor
field.value.transformer.fields=ssn,email
field.value.transformer.oracle.key=your-oracle-key
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

## Production Considerations

1. **Key Management**: Use a proper KMS (AWS KMS, Azure Key Vault, GCP KMS) instead of local keys
2. **Key Rotation**: Implement a key rotation strategy
3. **Performance**: CS-FLE adds encryption overhead; test with your workload
4. **Monitoring**: Monitor connector performance and error rates
5. **Security**: Ensure encryption keys are never logged or exposed

## Learn More

- [MongoDB Client-Side Field Level Encryption](https://www.mongodb.com/docs/manual/core/csfle/)
- [MongoDB Kafka Connector Documentation](https://www.mongodb.com/docs/kafka-connector/current/)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)

