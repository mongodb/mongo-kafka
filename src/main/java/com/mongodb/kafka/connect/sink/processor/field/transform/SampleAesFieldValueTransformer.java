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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * A sample {@link FieldValueTransformer} that decrypts AES-encrypted, Base64-encoded field values.
 *
 * <p>This is a <strong>sample implementation</strong> intended as a reference for customers who need
 * to provide their own decryption logic. In a real scenario, the customer would replace this class
 * with one that implements their proprietary decryption algorithm.
 *
 * <p>Configuration properties:
 *
 * <ul>
 *   <li>{@code field.value.transformer.aes.key} — A 16, 24, or 32 character AES key (required).
 *   <li>{@code field.value.transformer.aes.algorithm} — The AES transformation string (default:
 *       {@code AES/ECB/PKCS5Padding}).
 * </ul>
 *
 * <p>Example connector configuration:
 *
 * <pre>
 * field.value.transformer=com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer
 * field.value.transformer.fields=ssn,credit_card
 * field.value.transformer.aes.key=mySecretKey12345
 * </pre>
 */
public class SampleAesFieldValueTransformer implements FieldValueTransformer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SampleAesFieldValueTransformer.class);

  public static final String AES_KEY_CONFIG = "field.value.transformer.aes.key";
  public static final String AES_ALGORITHM_CONFIG = "field.value.transformer.aes.algorithm";
  private static final String DEFAULT_ALGORITHM = "AES/ECB/PKCS5Padding";

  private SecretKeySpec secretKey;
  private String algorithm;

  public SampleAesFieldValueTransformer() {
    // required public no-arg constructor
  }

  @Override
  public void init(final Map<String, String> configs) {
    String key = configs.get(AES_KEY_CONFIG);
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException(
          AES_KEY_CONFIG + " must be provided for AES decryption");
    }
    this.secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "AES");
    this.algorithm = configs.getOrDefault(AES_ALGORITHM_CONFIG, DEFAULT_ALGORITHM);
    LOGGER.info("SampleAesFieldValueTransformer initialized with algorithm: {}", algorithm);
  }

  @Override
  public BsonValue transform(final String fieldName, final BsonValue value) {
    if (!value.isString()) {
      LOGGER.debug(
          "Field '{}' is not a string (type={}), skipping decryption", fieldName, value.getBsonType());
      return value;
    }

    String encrypted = value.asString().getValue();
    try {
      byte[] encryptedBytes = Base64.getDecoder().decode(encrypted);
      Cipher cipher = Cipher.getInstance(algorithm);
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      byte[] decrypted = cipher.doFinal(encryptedBytes);
      String plaintext = new String(decrypted, StandardCharsets.UTF_8);
      LOGGER.debug("Successfully decrypted field '{}'", fieldName);
      return new BsonString(plaintext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt field '" + fieldName + "'", e);
    }
  }
}

