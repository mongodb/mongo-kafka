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

package com.mongodb.kafka.connect.sink.processor.field.transform;

import static com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer.AES_ALGORITHM_CONFIG;
import static com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer.AES_KEY_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/** Unit tests for {@link SampleAesFieldValueTransformer}. */
class SampleAesFieldValueTransformerTest {

  private static final String TEST_AES_KEY = "mySecretKey12345"; // 16 bytes for AES-128

  /** Helper to AES-encrypt and Base64-encode a plaintext string. */
  private static String encrypt(final String plaintext, final String key) throws Exception {
    SecretKeySpec keySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "AES");
    Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
    cipher.init(Cipher.ENCRYPT_MODE, keySpec);
    byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(encrypted);
  }

  private SampleAesFieldValueTransformer createTransformer(final String key) {
    SampleAesFieldValueTransformer transformer = new SampleAesFieldValueTransformer();
    Map<String, String> configs = new HashMap<>();
    configs.put(AES_KEY_CONFIG, key);
    transformer.init(configs);
    return transformer;
  }

  @Test
  @DisplayName("test decrypts AES-encrypted value")
  void testDecryptsValue() throws Exception {
    SampleAesFieldValueTransformer transformer = createTransformer(TEST_AES_KEY);
    String encrypted = encrypt("hello world", TEST_AES_KEY);
    BsonValue result = transformer.transform("field1", new BsonString(encrypted));
    assertEquals("hello world", result.asString().getValue());
  }

  @Test
  @DisplayName("test decrypts multiple different values")
  void testDecryptsMultipleValues() throws Exception {
    SampleAesFieldValueTransformer transformer = createTransformer(TEST_AES_KEY);

    String[] plaintexts = {"SSN-123-45-6789", "credit-card-4111", "secret@email.com", ""};
    for (String plaintext : plaintexts) {
      String encrypted = encrypt(plaintext, TEST_AES_KEY);
      BsonValue result = transformer.transform("field", new BsonString(encrypted));
      assertEquals(plaintext, result.asString().getValue());
    }
  }

  @Test
  @DisplayName("test skips non-string values")
  void testSkipsNonStringValues() {
    SampleAesFieldValueTransformer transformer = createTransformer(TEST_AES_KEY);
    BsonInt32 intValue = new BsonInt32(42);
    BsonValue result = transformer.transform("field", intValue);
    assertEquals(intValue, result);
  }

  @Test
  @DisplayName("test throws on invalid encrypted data")
  void testThrowsOnInvalidData() {
    SampleAesFieldValueTransformer transformer = createTransformer(TEST_AES_KEY);
    // Valid Base64 but not valid AES ciphertext
    assertThrows(
        RuntimeException.class,
        () -> transformer.transform("field", new BsonString("notActuallyEncrypted==")));
  }

  @Test
  @DisplayName("test throws on wrong key")
  void testThrowsOnWrongKey() throws Exception {
    String encrypted = encrypt("hello", TEST_AES_KEY);
    SampleAesFieldValueTransformer transformer = createTransformer("differentKey12345");
    assertThrows(
        RuntimeException.class, () -> transformer.transform("field", new BsonString(encrypted)));
  }

  @Test
  @DisplayName("test init throws when key is missing")
  void testInitThrowsWhenKeyMissing() {
    SampleAesFieldValueTransformer transformer = new SampleAesFieldValueTransformer();
    Map<String, String> configs = new HashMap<>();
    assertThrows(IllegalArgumentException.class, () -> transformer.init(configs));
  }

  @Test
  @DisplayName("test init throws when key is empty")
  void testInitThrowsWhenKeyEmpty() {
    SampleAesFieldValueTransformer transformer = new SampleAesFieldValueTransformer();
    Map<String, String> configs = new HashMap<>();
    configs.put(AES_KEY_CONFIG, "");
    assertThrows(IllegalArgumentException.class, () -> transformer.init(configs));
  }

  @Test
  @DisplayName("test custom algorithm config")
  void testCustomAlgorithmConfig() throws Exception {
    SampleAesFieldValueTransformer transformer = new SampleAesFieldValueTransformer();
    Map<String, String> configs = new HashMap<>();
    configs.put(AES_KEY_CONFIG, TEST_AES_KEY);
    configs.put(AES_ALGORITHM_CONFIG, "AES/ECB/PKCS5Padding");
    transformer.init(configs);

    String encrypted = encrypt("custom algo test", TEST_AES_KEY);
    BsonValue result = transformer.transform("field", new BsonString(encrypted));
    assertEquals("custom algo test", result.asString().getValue());
  }
}

