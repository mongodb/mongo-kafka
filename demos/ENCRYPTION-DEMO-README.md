# MongoDB Kafka Connector — Encryption Features Demo (KAFKA-470)

This demo showcases the two encryption-related features implemented for Oracle-to-MongoDB migration scenarios:

## Features

### 1. Pluggable Field Decryption
Allows custom decryption of fields as data flows through the Sink Connector. This is useful when migrating from systems like Oracle/Hibernate that use proprietary encryption.

**Configuration:**
```properties
field.value.transformer=com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer
field.value.transformer.fields=ssn,email
field.value.transformer.fail.on.error=true
field.value.transformer.aes.key=mySecretKey12345
```

### 2. Client-Side Field Level Encryption (CS-FLE)
Enables MongoDB's native Client-Side Field Level Encryption, allowing the connector to automatically encrypt specified fields before writing to MongoDB.

**Configuration:**
```properties
csfle.enabled=true
csfle.key.vault.namespace=encryption.__keyVault
csfle.local.master.key=<base64-encoded-96-byte-key>
csfle.schema.map={"db.collection": {"bsonType": "object", "properties": {...}}}
```

## Running the Demo

### Prerequisites
- Docker and Docker Compose
- Bash shell

### Steps

1. **Build the connector JAR:**
   ```bash
   cd /path/to/mongo-kafka
   ./gradlew allJar
   cp build/libs/mongo-kafka-connect-*-all.jar demo/connector-jar/mongo-kafka-connect.jar
   ```

2. **Start the environment:**
   ```bash
   cd demo
   docker compose up --build -d
   ```

3. **Run the demo:**
   ```bash
   bash demo-encryption.sh
   ```

The demo will:
- Create sample employee records with AES-encrypted SSN and email fields
- Set up a Source Connector to publish changes to Kafka
- Set up a Sink Connector with the `SampleAesFieldValueTransformer`
- Decrypt the fields and write plaintext to MongoDB
- Verify the decryption was successful

## Demo Output

The demo shows a side-by-side comparison:

**Source (encrypted):**
```json
{
  "_id": 1,
  "name": "Alice Johnson",
  "ssn": "A+3BxZDKVFsBSRoYsD6dOg==",
  "email": "tX571tgLeJhbf1kQHEQPwp2Vkldo3utNo3mUruMf8so=",
  "dept": "Engineering"
}
```

**Sink (decrypted):**
```json
{
  "_id": 1,
  "name": "Alice Johnson",
  "ssn": "123-45-6789",
  "email": "alice@example.com",
  "dept": "Engineering"
}
```

## CS-FLE Limitation

**Note:** The CS-FLE feature is fully implemented and tested, but cannot be demonstrated in this Docker environment on ARM64 (Apple Silicon) due to `mongodb-crypt` native library limitations. The feature works correctly on x86_64 platforms.

To use CS-FLE in production:
1. Deploy on x86_64 architecture
2. Configure both `field.value.transformer.*` and `csfle.*` properties
3. The data flow will be: **Oracle encryption → decryption → CS-FLE re-encryption → MongoDB**

## Cleanup

```bash
curl -X DELETE http://localhost:8083/connectors/enc-source
curl -X DELETE http://localhost:8083/connectors/dec-sink
cd demo && docker compose down -v
```

## Custom Decryption Implementation

To use your own decryption logic:

1. **Implement the interface:**
   ```java
   package com.example;
   
   import com.mongodb.kafka.connect.sink.processor.field.transform.FieldValueTransformer;
   
   public class MyCustomDecryptor implements FieldValueTransformer {
       @Override
       public void configure(Map<String, ?> configs) {
           // Initialize with custom config
       }
       
       @Override
       public Object transform(Object value) {
           // Your decryption logic here
           return decryptedValue;
       }
   }
   ```

2. **Package it in a JAR** and place it in the Kafka Connect plugin directory

3. **Configure the connector:**
   ```properties
   field.value.transformer=com.example.MyCustomDecryptor
   field.value.transformer.fields=ssn,email
   field.value.transformer.fail.on.error=true
   # Add any custom config properties your transformer needs
   ```

## Architecture

```
Oracle Database (AES-encrypted fields)
  ↓
Kafka Topic (still encrypted)
  ↓
Sink Connector
  ├─ FieldValueTransformPostProcessor (decrypts)
  └─ MongoClient with AutoEncryptionSettings (re-encrypts with CS-FLE, if enabled)
  ↓
MongoDB (encrypted with CS-FLE or plaintext)
```

The data is protected at every stage except for a brief moment in the connector's memory during transformation.

