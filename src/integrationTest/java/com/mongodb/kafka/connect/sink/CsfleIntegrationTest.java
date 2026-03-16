/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.sink;

import static java.lang.String.format;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;

import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;

/**
 * Integration tests for Client-Side Field Level Encryption (CS-FLE) functionality.
 *
 * <p>These tests verify that CS-FLE correctly encrypts configured fields before storing data in
 * MongoDB.
 */
public class CsfleIntegrationTest extends MongoKafkaTestCase {

  private static final String TOPIC = "csfle-test";
  private static final String KEY_VAULT_DB = "encryption";
  private static final String KEY_VAULT_COLL = "__keyVault";
  private static final String KEY_VAULT_NAMESPACE = KEY_VAULT_DB + "." + KEY_VAULT_COLL;

  private byte[] localMasterKey;
  private BsonBinary dataKeyId;

  @BeforeEach
  void setUp() {
    // Skip tests if mongodb-crypt is not available
    try {
      Class.forName("com.mongodb.crypt.capi.MongoCrypt");
    } catch (ClassNotFoundException e) {
      assumeTrue(false, "Skipping CS-FLE tests: mongodb-crypt library not available");
    }

    cleanUp();

    // Generate a 96-byte local master key
    localMasterKey = new byte[96];
    new java.security.SecureRandom().nextBytes(localMasterKey);

    // Check if CSFLE automatic encryption is available
    if (!isCsfleAvailable()) {
      return; // Skip data key creation if CSFLE not available
    }

    // Create data encryption key
    createDataEncryptionKey();
  }

  /**
   * Checks if CSFLE automatic encryption is available by attempting to create a test client.
   * Returns false if mongocryptd or crypt_shared library is not available.
   */
  private boolean isCsfleAvailable() {
    try {
      Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
      Map<String, Object> localMasterKeyMap = new HashMap<>();
      localMasterKeyMap.put("key", localMasterKey);
      kmsProviders.put("local", localMasterKeyMap);

      com.mongodb.AutoEncryptionSettings testSettings =
          com.mongodb.AutoEncryptionSettings.builder()
              .keyVaultNamespace(KEY_VAULT_NAMESPACE)
              .kmsProviders(kmsProviders)
              .build();

      MongoClientSettings testClientSettings =
          MongoClientSettings.builder()
              .applyConnectionString(MONGODB.getConnectionString())
              .autoEncryptionSettings(testSettings)
              .build();

      try (MongoClient testClient = MongoClients.create(testClientSettings)) {
        // Try to perform a simple operation to trigger library loading
        testClient.getDatabase("test").listCollectionNames().first();
        return true;
      }
    } catch (Exception e) {
      // CSFLE libraries not available
      return false;
    }
  }

  @AfterEach
  void tearDown() {
    cleanUp();
    // Clean up key vault
    try {
      getMongoClient().getDatabase(KEY_VAULT_DB).drop();
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  @DisplayName("Ensure CS-FLE encrypts configured fields")
  void testCsfleEncryptsConfiguredFields() {
    assumeTrue(
        isCsfleAvailable(),
        "Skipping CSFLE test: mongocryptd or crypt_shared library not available");

    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(MongoSinkConfig.CSFLE_ENABLED_CONFIG, "true");
      cfg.put(MongoSinkConfig.CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NAMESPACE);
      cfg.put(
          MongoSinkConfig.CSFLE_LOCAL_MASTER_KEY_CONFIG,
          Base64.getEncoder().encodeToString(localMasterKey));
      cfg.put(MongoSinkConfig.CSFLE_SCHEMA_MAP_CONFIG, createSchemaMap());
      cfg.put(
          MongoSinkConfig.CSFLE_BYPASS_QUERY_ANALYSIS_CONFIG,
          "false"); // Enable automatic encryption
      task.start(cfg);

      // Create documents with plaintext fields
      List<Document> documents =
          rangeClosed(1, 3)
              .mapToObj(
                  i ->
                      new Document("_id", i)
                          .append("name", "Employee " + i)
                          .append("ssn", "123-45-" + String.format("%04d", i))
                          .append("email", "employee" + i + "@example.com")
                          .append("dept", "Engineering"))
              .collect(Collectors.toList());

      List<SinkRecord> sinkRecords = createRecords(documents);
      task.put(sinkRecords);

      // Verify documents are stored
      assertEquals(3, getCollection().countDocuments());

      // Verify encrypted fields are actually encrypted (Binary subtype 6)
      Document doc = getCollection().find(new Document("_id", 1)).first();
      assertNotNull(doc);
      assertEquals("Employee 1", doc.getString("name")); // Not encrypted
      assertEquals("Engineering", doc.getString("dept")); // Not encrypted

      // SSN and email should be encrypted (Binary subtype 6)
      Object ssnValue = doc.get("ssn");
      Object emailValue = doc.get("email");
      assertTrue(ssnValue instanceof org.bson.types.Binary, "SSN should be encrypted");
      assertTrue(emailValue instanceof org.bson.types.Binary, "Email should be encrypted");

      org.bson.types.Binary ssnBinary = (org.bson.types.Binary) ssnValue;
      org.bson.types.Binary emailBinary = (org.bson.types.Binary) emailValue;
      assertEquals(6, ssnBinary.getType(), "SSN should use Binary subtype 6 (encrypted)");
      assertEquals(6, emailBinary.getType(), "Email should use Binary subtype 6 (encrypted)");
    }
  }

  @Test
  @DisplayName("Ensure CS-FLE can decrypt encrypted fields with proper client")
  void testCsfleCanDecryptWithProperClient() {
    assumeTrue(
        isCsfleAvailable(),
        "Skipping CSFLE test: mongocryptd or crypt_shared library not available");

    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(MongoSinkConfig.CSFLE_ENABLED_CONFIG, "true");
      cfg.put(MongoSinkConfig.CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NAMESPACE);
      cfg.put(
          MongoSinkConfig.CSFLE_LOCAL_MASTER_KEY_CONFIG,
          Base64.getEncoder().encodeToString(localMasterKey));
      cfg.put(MongoSinkConfig.CSFLE_SCHEMA_MAP_CONFIG, createSchemaMap());
      cfg.put(
          MongoSinkConfig.CSFLE_BYPASS_QUERY_ANALYSIS_CONFIG,
          "false"); // Enable automatic encryption
      task.start(cfg);

      Document document =
          new Document("_id", 1)
              .append("name", "John Doe")
              .append("ssn", "987-65-4321")
              .append("email", "john.doe@example.com");

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(document));
      task.put(sinkRecords);

      // Read with encryption-enabled client
      try (MongoClient encryptedClient = createEncryptedMongoClient()) {
        MongoCollection<Document> encryptedColl =
            encryptedClient
                .getDatabase(getCollection().getNamespace().getDatabaseName())
                .getCollection(getCollection().getNamespace().getCollectionName());

        Document decrypted = encryptedColl.find(new Document("_id", 1)).first();
        assertNotNull(decrypted);
        assertEquals("987-65-4321", decrypted.getString("ssn"));
        assertEquals("john.doe@example.com", decrypted.getString("email"));
      }
    }
  }

  @Test
  @DisplayName("Ensure CS-FLE does not encrypt non-configured fields")
  void testCsfleDoesNotEncryptNonConfiguredFields() {
    assumeTrue(
        isCsfleAvailable(),
        "Skipping CSFLE test: mongocryptd or crypt_shared library not available");

    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(MongoSinkConfig.CSFLE_ENABLED_CONFIG, "true");
      cfg.put(MongoSinkConfig.CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NAMESPACE);
      cfg.put(
          MongoSinkConfig.CSFLE_LOCAL_MASTER_KEY_CONFIG,
          Base64.getEncoder().encodeToString(localMasterKey));
      cfg.put(MongoSinkConfig.CSFLE_SCHEMA_MAP_CONFIG, createSchemaMap());
      cfg.put(
          MongoSinkConfig.CSFLE_BYPASS_QUERY_ANALYSIS_CONFIG,
          "false"); // Enable automatic encryption
      task.start(cfg);

      Document document =
          new Document("_id", 1)
              .append("name", "Jane Smith")
              .append("dept", "Sales")
              .append("ssn", "111-22-3333");

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(document));
      task.put(sinkRecords);

      Document stored = getCollection().find(new Document("_id", 1)).first();
      assertNotNull(stored);

      // name and dept should NOT be encrypted
      assertEquals("Jane Smith", stored.getString("name"));
      assertEquals("Sales", stored.getString("dept"));

      // ssn SHOULD be encrypted
      assertTrue(stored.get("ssn") instanceof org.bson.types.Binary);
    }
  }

  private void createDataEncryptionKey() {
    Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
    Map<String, Object> localMasterKeyMap = new HashMap<>();
    localMasterKeyMap.put("key", localMasterKey);
    kmsProviders.put("local", localMasterKeyMap);

    ClientEncryptionSettings encryptionSettings =
        ClientEncryptionSettings.builder()
            .keyVaultMongoClientSettings(
                MongoClientSettings.builder()
                    .applyConnectionString(MONGODB.getConnectionString())
                    .build())
            .keyVaultNamespace(KEY_VAULT_NAMESPACE)
            .kmsProviders(kmsProviders)
            .build();

    try (ClientEncryption clientEncryption = ClientEncryptions.create(encryptionSettings)) {
      dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
    }
  }

  private MongoClient createEncryptedMongoClient() {
    Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
    Map<String, Object> localMasterKeyMap = new HashMap<>();
    localMasterKeyMap.put("key", localMasterKey);
    kmsProviders.put("local", localMasterKeyMap);

    Map<String, BsonDocument> schemaMap = new HashMap<>();
    schemaMap.put(
        getCollection().getNamespace().getFullName(), BsonDocument.parse(createSchemaMap()));

    com.mongodb.AutoEncryptionSettings autoEncryptionSettings =
        com.mongodb.AutoEncryptionSettings.builder()
            .keyVaultNamespace(KEY_VAULT_NAMESPACE)
            .kmsProviders(kmsProviders)
            .schemaMap(schemaMap)
            .build();

    MongoClientSettings clientSettings =
        MongoClientSettings.builder()
            .applyConnectionString(MONGODB.getConnectionString())
            .autoEncryptionSettings(autoEncryptionSettings)
            .build();

    return MongoClients.create(clientSettings);
  }

  private String createSchemaMap() {
    String namespace = getCollection().getNamespace().getFullName();
    return format(
        "{\"%s\": {"
            + "  \"bsonType\": \"object\","
            + "  \"properties\": {"
            + "    \"ssn\": {"
            + "      \"encrypt\": {"
            + "        \"keyId\": [{\"$binary\": {\"base64\": \"%s\", \"subType\": \"04\"}}],"
            + "        \"bsonType\": \"string\","
            + "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
            + "      }"
            + "    },"
            + "    \"email\": {"
            + "      \"encrypt\": {"
            + "        \"keyId\": [{\"$binary\": {\"base64\": \"%s\", \"subType\": \"04\"}}],"
            + "        \"bsonType\": \"string\","
            + "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
            + "      }"
            + "    }"
            + "  }"
            + "}}",
        namespace,
        Base64.getEncoder().encodeToString(dataKeyId.getData()),
        Base64.getEncoder().encodeToString(dataKeyId.getData()));
  }

  private AutoCloseableSinkTask createSinkTask() {
    return new AutoCloseableSinkTask(new MongoSinkTask());
  }

  private Map<String, String> createSettings() {
    return new HashMap<String, String>() {
      {
        put(MongoSinkConfig.TOPICS_CONFIG, TOPIC);
        put(MongoSinkTopicConfig.DATABASE_CONFIG, getCollection().getNamespace().getDatabaseName());
        put(
            MongoSinkTopicConfig.COLLECTION_CONFIG,
            getCollection().getNamespace().getCollectionName());
      }
    };
  }

  private List<SinkRecord> createRecords(final List<Document> documents) {
    return documents.stream()
        .map(
            doc ->
                new SinkRecord(
                    TOPIC,
                    0,
                    org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                    doc.toJson(),
                    org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                    doc.toJson(),
                    doc.get("_id", 0) instanceof Number
                        ? ((Number) doc.get("_id", 0)).longValue()
                        : 0L))
        .collect(Collectors.toList());
  }

  /** Helper class to wrap MongoSinkTask for use in try-with-resources. */
  static class AutoCloseableSinkTask extends SinkTask implements AutoCloseable {

    private final MongoSinkTask wrapped;

    AutoCloseableSinkTask(final MongoSinkTask wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public void close() {
      wrapped.stop();
    }

    @Override
    public String version() {
      return wrapped.version();
    }

    @Override
    public void start(final Map<String, String> overrides) {
      HashMap<String, String> props = new HashMap<>();
      props.put(MongoSinkConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
      props.putAll(overrides);
      wrapped.start(props);
    }

    @Override
    public void put(final java.util.Collection<SinkRecord> records) {
      wrapped.put(records);
    }

    @Override
    public void stop() {
      wrapped.stop();
    }
  }
}
