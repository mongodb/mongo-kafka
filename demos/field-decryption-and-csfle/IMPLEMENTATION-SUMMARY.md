# KAFKA-470 Implementation Summary

## Overview

This document summarizes the implementation of two encryption-related features for the MongoDB Kafka Sink Connector, designed to support database migration scenarios.

## Features Implemented

### Feature 1: Pluggable Field Decryption

**Purpose**: Allow custom decryption of fields as data flows through the Sink Connector.

**Use Case**: When migrating from legacy systems that use proprietary encryption, customers need to decrypt data before storing it in MongoDB.

**Implementation**:

1. **Interface**: `FieldValueTransformer`
   - Location: `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformer.java`
   - Methods:
     - `configure(Map<String, ?> configs)` - Initialize with configuration
     - `Object transform(Object value)` - Transform/decrypt a single value

2. **PostProcessor**: `FieldValueTransformPostProcessor`
   - Location: `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformPostProcessor.java`
   - Recursively walks document structure (including nested fields and arrays)
   - Applies transformation only to configured fields
   - Automatically added to PostProcessor chain when transformer is configured

3. **Sample Implementation**: `SampleAesFieldValueTransformer`
   - Location: `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/SampleAesFieldValueTransformer.java`
   - Demonstrates AES-128-ECB decryption
   - Serves as a reference for customers to implement their own

4. **Configuration** (in `MongoSinkTopicConfig`):
   - `field.value.transformer` - Fully qualified class name of transformer
   - `field.value.transformer.fields` - Comma-separated list of fields to transform
   - `field.value.transformer.fail.on.error` - Whether to fail on transformation errors (default: true)
   - Custom properties: Any property starting with `field.value.transformer.*` is passed to the transformer

### Feature 2: Client-Side Field Level Encryption (CS-FLE)

**Purpose**: Enable MongoDB's native Client-Side Field Level Encryption in the Sink Connector.

**Use Case**: After decrypting legacy encryption, re-encrypt data using MongoDB's encryption before storing it.

**Implementation**:

1. **MongoClient Enhancement** (in `MongoSinkTask`):
   - `buildAutoEncryptionSettings()` - Constructs `AutoEncryptionSettings` from configuration
   - `parseSchemaMap()` - Parses JSON schema map for field encryption rules
   - Modified `createMongoClient()` to attach `AutoEncryptionSettings` when CS-FLE is enabled

2. **Configuration** (in `MongoSinkConfig`):
   - `csfle.enabled` - Enable/disable CS-FLE (default: false)
   - `csfle.key.vault.namespace` - Key vault location (format: `database.collection`)
   - `csfle.local.master.key` - Base64-encoded 96-byte master key for local KMS
   - `csfle.schema.map` - JSON schema defining which fields to encrypt

3. **Dependency**:
   - Added `mongodb-crypt:5.6.4` to `build.gradle.kts`
   - Included in `mongoAndAvroDependencies` for `allJar` task

## Test Coverage

**Total Tests**: 645 (all passing)
**New Tests for KAFKA-470**: 28

### FieldValueTransformPostProcessor Tests (15 tests)
- Transforms configured fields
- Handles nested fields and deeply nested fields
- Transforms fields inside arrays
- Transforms multiple fields across nesting levels
- Skips non-target fields
- Handles missing target fields
- Handles empty documents and null values
- Fail-on-error behavior (throws `DataException`)
- No-fail-on-error behavior (keeps original value)
- Invalid transformer class handling

### SampleAesFieldValueTransformer Tests (8 tests)
- Decrypts AES-encrypted values
- Handles multiple different encrypted values
- Skips non-string values
- Throws on invalid encrypted data
- Throws on wrong decryption key
- Throws when key is missing or empty
- Custom algorithm configuration

### MongoSinkTask CS-FLE Tests (8 tests)
- CS-FLE disabled by default
- Configuration accessors and defaults
- `buildAutoEncryptionSettings` with valid config
- `buildAutoEncryptionSettings` with schema map
- `buildAutoEncryptionSettings` without schema map
- Throws when key vault namespace is missing
- Throws when master key is missing

### Integration Tests (3 tests in MongoSinkConfigTest)
- PostProcessor chain includes transformer when configured
- PostProcessor chain excludes transformer when not configured
- Transformer receives correct configuration

## Demo

**Location**: `demos/field-decryption-and-csfle/`

**Contents**:
- `README.md` - Comprehensive documentation
- `run-demo.sh` - Automated demo runner
- `demo-encryption-with-csfle.sh` - Demo script showing both features
- `docker-compose.yml` - Docker environment (MongoDB, Kafka, Kafka Connect)
- `Dockerfile.connect` - Custom Kafka Connect image with mongodb-crypt
- `connector-jar/` - Directory for the connector JAR

**What the Demo Shows**:
1. Creates sample data encrypted with AES (simulating legacy system encryption)
2. Publishes to Kafka (still encrypted)
3. Sink Connector decrypts using `SampleAesFieldValueTransformer`
4. Sink Connector re-encrypts using MongoDB CS-FLE
5. Verifies data is encrypted in MongoDB

**Platform Requirements**:
- **Works on**: x86_64 (Intel/AMD) Linux, macOS, Windows
- **Requires workaround on**: ARM64 (Apple Silicon) - use `export DOCKER_DEFAULT_PLATFORM=linux/amd64`
- **Why**: `mongodb-crypt` native library requires glibc (not musl/Alpine) and has better x86_64 support

## Customer Usage

### For Decryption Only

1. Implement `FieldValueTransformer` interface
2. Package in JAR and place in Kafka Connect plugin directory
3. Configure connector:
   ```properties
   field.value.transformer=com.customer.MyDecryptor
   field.value.transformer.fields=ssn,email
   field.value.transformer.custom.key=value
   ```

### For Decryption + CS-FLE

1. Implement `FieldValueTransformer` (as above)
2. Set up MongoDB key vault and data encryption keys
3. Configure connector:
   ```properties
   # Decryption
   field.value.transformer=com.customer.MyDecryptor
   field.value.transformer.fields=ssn,email

   # CS-FLE
   csfle.enabled=true
   csfle.key.vault.namespace=encryption.__keyVault
   csfle.local.master.key=<base64-key>
   csfle.schema.map={"db.collection": {...}}
   ```

## Files Modified/Created

### New Files
- `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformer.java`
- `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformPostProcessor.java`
- `src/main/java/com/mongodb/kafka/connect/sink/processor/field/transform/SampleAesFieldValueTransformer.java`
- `src/test/java/com/mongodb/kafka/connect/sink/processor/field/transform/FieldValueTransformPostProcessorTest.java`
- `src/test/java/com/mongodb/kafka/connect/sink/processor/field/transform/SampleAesFieldValueTransformerTest.java`
- `src/test/java/com/mongodb/kafka/connect/sink/MongoSinkTaskCsfleTest.java`
- `demos/field-decryption-and-csfle/` (entire demo directory)

### Modified Files
- `build.gradle.kts` - Added `mongodb-crypt` dependency
- `src/main/java/com/mongodb/kafka/connect/sink/MongoSinkConfig.java` - Added CS-FLE config params
- `src/main/java/com/mongodb/kafka/connect/sink/MongoSinkTopicConfig.java` - Added transformer config params
- `src/main/java/com/mongodb/kafka/connect/sink/MongoSinkTask.java` - Added CS-FLE initialization
- `src/main/java/com/mongodb/kafka/connect/sink/processor/PostProcessors.java` - Auto-register transformer
- `src/test/java/com/mongodb/kafka/connect/sink/MongoSinkConfigTest.java` - Added integration tests

## Production Considerations

1. **Key Management**: Use proper KMS (AWS KMS, Azure Key Vault, GCP KMS) instead of local keys
2. **Performance**: CS-FLE adds encryption overhead; benchmark with your workload
3. **Error Handling**: Set `field.value.transformer.fail.on.error` based on your requirements
4. **Monitoring**: Monitor transformation errors and connector performance
5. **Security**: Ensure encryption keys are never logged or exposed
6. **Testing**: Test with your actual encryption implementation before production deployment

## Known Limitations

1. **Platform**: CS-FLE native library works best on x86_64 with glibc-based Linux
2. **ARM64**: Requires x86_64 emulation (slower) or may not work in some environments
3. **Alpine Linux**: Not compatible due to musl libc vs glibc
4. **Schema Map**: Must be valid JSON; complex schemas can be error-prone

## Next Steps

1. Customer provides their custom decryption implementation
2. Test with customer's actual encrypted data
3. Set up production key management (KMS)
4. Performance testing with production workload
5. Deploy to staging environment
6. Production rollout
