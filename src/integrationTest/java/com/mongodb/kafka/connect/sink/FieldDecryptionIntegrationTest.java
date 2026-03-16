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

import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.Document;

import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;

/**
 * Integration tests for field decryption functionality.
 *
 * <p>These tests verify that the FieldValueTransformer interface and SampleAesFieldValueTransformer
 * correctly decrypt encrypted fields before storing data in MongoDB.
 */
public class FieldDecryptionIntegrationTest extends MongoKafkaTestCase {

  private static final String TOPIC = "field-decryption-test";
  private static final String AES_KEY = "mySecretKey12345"; // 16 bytes for AES-128

  @BeforeEach
  void setUp() {
    cleanUp();
  }

  @AfterEach
  void tearDown() {
    cleanUp();
  }

  @Test
  @DisplayName("Ensure field decryption transforms encrypted fields")
  void testFieldDecryptionTransformsEncryptedFields() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer");
      cfg.put(MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG, "ssn,email");
      cfg.put("field.value.transformer.aes.key", AES_KEY);
      task.start(cfg);

      // Create documents with encrypted fields
      List<Document> documents =
          rangeClosed(1, 5)
              .mapToObj(
                  i ->
                      new Document("_id", i)
                          .append("name", "Employee " + i)
                          .append("ssn", encryptAes("123-45-" + String.format("%04d", i)))
                          .append("email", encryptAes("employee" + i + "@example.com"))
                          .append("dept", "Engineering"))
              .collect(Collectors.toList());

      List<SinkRecord> sinkRecords = createRecords(documents);
      task.put(sinkRecords);

      // Verify documents are stored with decrypted values
      assertEquals(5, getCollection().countDocuments());
      Document doc = getCollection().find(new Document("_id", 1)).first();
      assertEquals("123-45-0001", doc.getString("ssn"));
      assertEquals("employee1@example.com", doc.getString("email"));
      assertEquals("Engineering", doc.getString("dept"));
    }
  }

  @Test
  @DisplayName("Ensure field decryption only transforms configured fields")
  void testFieldDecryptionOnlyTransformsConfiguredFields() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer");
      cfg.put(MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG, "ssn"); // Only SSN
      cfg.put("field.value.transformer.aes.key", AES_KEY);
      task.start(cfg);

      String encryptedSsn = encryptAes("123-45-6789");
      String encryptedEmail = encryptAes("test@example.com");

      Document document =
          new Document("_id", 1).append("ssn", encryptedSsn).append("email", encryptedEmail);

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(document));
      task.put(sinkRecords);

      Document stored = getCollection().find(new Document("_id", 1)).first();
      assertEquals("123-45-6789", stored.getString("ssn")); // Decrypted
      assertEquals(encryptedEmail, stored.getString("email")); // Still encrypted
    }
  }

  @Test
  @DisplayName("Ensure field decryption works with nested fields")
  void testFieldDecryptionWithNestedFields() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer");
      cfg.put(MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG, "ssn");
      cfg.put("field.value.transformer.aes.key", AES_KEY);
      task.start(cfg);

      Document nested =
          new Document("_id", 1)
              .append("name", "John Doe")
              .append("personal", new Document("ssn", encryptAes("987-65-4321")).append("age", 30));

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(nested));
      task.put(sinkRecords);

      Document stored = getCollection().find(new Document("_id", 1)).first();
      Document personalInfo = (Document) stored.get("personal");
      assertEquals("987-65-4321", personalInfo.getString("ssn")); // Decrypted
      assertEquals(30, personalInfo.getInteger("age"));
    }
  }

  @Test
  @DisplayName("Ensure field decryption works with arrays")
  void testFieldDecryptionWithArrays() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer");
      cfg.put(MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG, "ssn");
      cfg.put("field.value.transformer.aes.key", AES_KEY);
      task.start(cfg);

      Document doc =
          new Document("_id", 1)
              .append("name", "Company")
              .append(
                  "employees",
                  Arrays.asList(
                      new Document("name", "Alice").append("ssn", encryptAes("111-11-1111")),
                      new Document("name", "Bob").append("ssn", encryptAes("222-22-2222"))));

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(doc));
      task.put(sinkRecords);

      Document stored = getCollection().find(new Document("_id", 1)).first();
      @SuppressWarnings("unchecked")
      List<Document> employees = (List<Document>) stored.get("employees");
      assertEquals("111-11-1111", employees.get(0).getString("ssn"));
      assertEquals("222-22-2222", employees.get(1).getString("ssn"));
    }
  }

  @Test
  @DisplayName("Ensure field decryption skips non-string values")
  void testFieldDecryptionSkipsNonStringValues() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer");
      cfg.put(MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG, "ssn");
      cfg.put("field.value.transformer.aes.key", AES_KEY);
      task.start(cfg);

      Document doc = new Document("_id", 1).append("ssn", 12345); // Integer, not string

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(doc));
      task.put(sinkRecords);

      Document stored = getCollection().find(new Document("_id", 1)).first();
      assertEquals(12345, stored.getInteger("ssn")); // Unchanged
    }
  }

  @Test
  @DisplayName("Ensure field decryption works with more than 2 fields and deeply nested subfields")
  void testFieldDecryptionWithMultipleFieldsAndDeeplyNestedSubfields() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer");
      // Configure 4 fields: ssn, email, secret, password
      cfg.put(
          MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG, "ssn,email,secret,password");
      cfg.put("field.value.transformer.aes.key", AES_KEY);
      task.start(cfg);

      // Create a complex document with multiple encrypted fields at different nesting
      // levels
      Document doc =
          new Document("_id", 1)
              .append("name", "John Doe") // Not encrypted
              .append("ssn", encryptAes("123-45-6789")) // Top-level encrypted
              .append("email", encryptAes("john@example.com")) // Top-level encrypted
              .append(
                  "profile",
                  new Document()
                      .append("age", 30) // Not encrypted
                      .append("secret", encryptAes("my-secret-data")) // Nested encrypted
                      .append(
                          "credentials",
                          new Document()
                              .append("username", "jdoe") // Not encrypted
                              .append(
                                  "password",
                                  encryptAes("super-secure-password")))) // Deeply nested encrypted
              .append(
                  "accounts",
                  Arrays.asList(
                      new Document()
                          .append("type", "checking") // Not encrypted
                          .append(
                              "secret", encryptAes("account-secret-1")), // Array element encrypted
                      new Document()
                          .append("type", "savings") // Not encrypted
                          .append(
                              "password",
                              encryptAes("account-password-2")))); // Array element encrypted

      List<SinkRecord> sinkRecords = createRecords(Collections.singletonList(doc));
      task.put(sinkRecords);

      Document stored = getCollection().find(new Document("_id", 1)).first();

      // Verify top-level encrypted fields are decrypted
      assertEquals("123-45-6789", stored.getString("ssn"));
      assertEquals("john@example.com", stored.getString("email"));

      // Verify non-encrypted top-level field is unchanged
      assertEquals("John Doe", stored.getString("name"));

      // Verify nested encrypted field is decrypted
      Document profile = stored.get("profile", Document.class);
      assertEquals("my-secret-data", profile.getString("secret"));
      assertEquals(30, profile.getInteger("age")); // Unchanged

      // Verify deeply nested encrypted field is decrypted
      Document credentials = profile.get("credentials", Document.class);
      assertEquals("super-secure-password", credentials.getString("password"));
      assertEquals("jdoe", credentials.getString("username")); // Unchanged

      // Verify encrypted fields in array elements are decrypted
      @SuppressWarnings("unchecked")
      List<Document> accounts = stored.get("accounts", List.class);
      assertEquals("account-secret-1", accounts.get(0).getString("secret"));
      assertEquals("checking", accounts.get(0).getString("type")); // Unchanged
      assertEquals("account-password-2", accounts.get(1).getString("password"));
      assertEquals("savings", accounts.get(1).getString("type")); // Unchanged
    }
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

  /**
   * Encrypts a plaintext string using AES-128-ECB.
   *
   * @param plaintext the plaintext to encrypt
   * @return Base64-encoded encrypted string
   */
  private String encryptAes(final String plaintext) {
    try {
      SecretKeySpec secretKey = new SecretKeySpec(AES_KEY.getBytes(StandardCharsets.UTF_8), "AES");
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(encrypted);
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt", e);
    }
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
