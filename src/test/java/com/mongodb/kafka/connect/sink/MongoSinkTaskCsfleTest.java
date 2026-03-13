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

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CSFLE_ENABLED_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CSFLE_KEY_VAULT_NAMESPACE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CSFLE_LOCAL_MASTER_KEY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CSFLE_SCHEMA_MAP_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createSinkConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.AutoEncryptionSettings;

/**
 * Unit tests for CS-FLE configuration in {@link MongoSinkConfig} and the {@link
 * MongoSinkTask#buildAutoEncryptionSettings} method.
 */
class MongoSinkTaskCsfleTest {

  // A valid 96-byte key, Base64-encoded
  private static final byte[] MASTER_KEY_BYTES = new byte[96];

  static {
    for (int i = 0; i < 96; i++) {
      MASTER_KEY_BYTES[i] = (byte) i;
    }
  }

  private static final String MASTER_KEY_BASE64 =
      Base64.getEncoder().encodeToString(MASTER_KEY_BYTES);
  private static final String KEY_VAULT_NS = "encryption.__keyVault";

  @Test
  @DisplayName("test CSFLE disabled by default")
  void testCsfleDisabledByDefault() {
    MongoSinkConfig config = createSinkConfig();
    assertFalse(config.isCsfleEnabled());
  }

  @Test
  @DisplayName("test CSFLE config accessors")
  void testCsfleConfigAccessors() {
    Map<String, String> map = createConfigMap();
    map.put(CSFLE_ENABLED_CONFIG, "true");
    map.put(CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NS);
    map.put(CSFLE_LOCAL_MASTER_KEY_CONFIG, MASTER_KEY_BASE64);
    map.put(CSFLE_SCHEMA_MAP_CONFIG, "{}");

    MongoSinkConfig config = new MongoSinkConfig(map);
    assertTrue(config.isCsfleEnabled());
    assertEquals(KEY_VAULT_NS, config.getCsfleKeyVaultNamespace());
    assertEquals(MASTER_KEY_BASE64, config.getCsfleLocalMasterKey());
    assertEquals("{}", config.getCsfleSchemaMap());
  }

  @Test
  @DisplayName("test CSFLE config defaults are empty strings")
  void testCsfleConfigDefaults() {
    MongoSinkConfig config = createSinkConfig();
    assertEquals("", config.getCsfleKeyVaultNamespace());
    assertEquals("", config.getCsfleLocalMasterKey());
    assertEquals("", config.getCsfleSchemaMap());
  }

  @Test
  @DisplayName("test buildAutoEncryptionSettings with valid config")
  void testBuildAutoEncryptionSettingsValid() {
    Map<String, String> map = createConfigMap();
    map.put(CSFLE_ENABLED_CONFIG, "true");
    map.put(CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NS);
    map.put(CSFLE_LOCAL_MASTER_KEY_CONFIG, MASTER_KEY_BASE64);

    MongoSinkConfig config = new MongoSinkConfig(map);
    AutoEncryptionSettings settings = MongoSinkTask.buildAutoEncryptionSettings(config);

    assertNotNull(settings);
    assertEquals(KEY_VAULT_NS, settings.getKeyVaultNamespace());
    assertNotNull(settings.getKmsProviders());
    assertTrue(settings.getKmsProviders().containsKey("local"));
  }

  @Test
  @DisplayName("test buildAutoEncryptionSettings with schema map")
  void testBuildAutoEncryptionSettingsWithSchemaMap() {
    Map<String, String> map = createConfigMap();
    map.put(CSFLE_ENABLED_CONFIG, "true");
    map.put(CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NS);
    map.put(CSFLE_LOCAL_MASTER_KEY_CONFIG, MASTER_KEY_BASE64);
    map.put(
        CSFLE_SCHEMA_MAP_CONFIG,
        "{'mydb.mycoll': {'bsonType': 'object', 'properties': {'ssn': {'encrypt': {'bsonType': 'string'}}}}}");

    MongoSinkConfig config = new MongoSinkConfig(map);
    AutoEncryptionSettings settings = MongoSinkTask.buildAutoEncryptionSettings(config);

    assertNotNull(settings);
    assertNotNull(settings.getSchemaMap());
    assertTrue(settings.getSchemaMap().containsKey("mydb.mycoll"));
  }

  @Test
  @DisplayName("test buildAutoEncryptionSettings throws when key vault missing")
  void testBuildAutoEncryptionSettingsThrowsWhenKeyVaultMissing() {
    Map<String, String> map = createConfigMap();
    map.put(CSFLE_ENABLED_CONFIG, "true");
    map.put(CSFLE_LOCAL_MASTER_KEY_CONFIG, MASTER_KEY_BASE64);

    MongoSinkConfig config = new MongoSinkConfig(map);
    ConnectException e =
        assertThrows(
            ConnectException.class, () -> MongoSinkTask.buildAutoEncryptionSettings(config));
    assertTrue(e.getMessage().contains("csfle.key.vault.namespace"));
  }

  @Test
  @DisplayName("test buildAutoEncryptionSettings throws when master key missing")
  void testBuildAutoEncryptionSettingsThrowsWhenMasterKeyMissing() {
    Map<String, String> map = createConfigMap();
    map.put(CSFLE_ENABLED_CONFIG, "true");
    map.put(CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NS);

    MongoSinkConfig config = new MongoSinkConfig(map);
    ConnectException e =
        assertThrows(
            ConnectException.class, () -> MongoSinkTask.buildAutoEncryptionSettings(config));
    assertTrue(e.getMessage().contains("csfle.local.master.key"));
  }

  @Test
  @DisplayName("test buildAutoEncryptionSettings with no schema map still works")
  void testBuildAutoEncryptionSettingsNoSchemaMap() {
    Map<String, String> map = createConfigMap();
    map.put(CSFLE_ENABLED_CONFIG, "true");
    map.put(CSFLE_KEY_VAULT_NAMESPACE_CONFIG, KEY_VAULT_NS);
    map.put(CSFLE_LOCAL_MASTER_KEY_CONFIG, MASTER_KEY_BASE64);

    MongoSinkConfig config = new MongoSinkConfig(map);
    AutoEncryptionSettings settings = MongoSinkTask.buildAutoEncryptionSettings(config);
    assertNotNull(settings);
    // Schema map should be empty/null when not configured
    assertTrue(settings.getSchemaMap().isEmpty());
  }
}
