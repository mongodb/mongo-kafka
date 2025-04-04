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
package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.util.ConfigHelper.getConfigByNameWithoutErrors;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;

public final class CsfleConfig {

  public static final String CSFLE_ENABLED_CONFIG = "csfle.enabled";
  private static final boolean CSFLE_ENABLED_DEFAULT = false;
  private static final String CSFLE_ENABLED_DISPLAY = "Enable CSFLE";
  private static final String CSFLE_ENABLED_DOC =
      "Enables Automatic Client-Side Field Level Encryption (CSFLE), restricted to local mode. Default is false.";

  public static final String CSFLE_MASTER_KEY_PATH_CONFIG = "csfle.master.key";
  private static final String CSFLE_MASTER_KEY_PATH_DEFAULT = "/etc/ssl/localKey";
  private static final String CSFLE_MASTER_KEY_PATH_DISPLAY = "Local Key Path";
  private static final String CSFLE_MASTER_KEY_PATH_DOC =
      "Specifies the master key path used for encryption.";

  public static final String CSFLE_CRYPT_SHARED_LIB_PATH_CONFIG = "csfle.cryptSharedLib.path";
  private static final String CSFLE_CRYPT_SHARED_LIB_PATH_DEFAULT = "/etc/ssl/mongo_crypt_v1.so";
  private static final String CSFLE_CRYPT_SHARED_LIB_PATH_DISPLAY = "CryptSharedLib Path";
  private static final String CSFLE_CRYPT_SHARED_LIB_PATH_DOC =
      "Crypt Shared Lib Path for automatic encryption";

  public static final String CSFLE_DATA_KEY_NAMESPACE_CONFIG = "csfle.datakey.path";
  private static final String CSFLE_DATA_KEY_NAMESPACE_DEFAULT = "encryption._keyvault";
  private static final String CSFLE_DATA_KEY_NAMESPACE_DISPLAY = "Data Key Vault Namespace";
  private static final String CSFLE_DATA_KEY_NAMESPACE_DOC = "Data key Vault Namespace for CSFLE";

  static final Logger LOGGER = LoggerFactory.getLogger(CsfleConfig.class);

  public static ConfigDef addCSFLEConfig(final ConfigDef configDef) {
    String group = "CSFLE";
    int orderInGroup = 0;
    configDef.define(
        CSFLE_ENABLED_CONFIG,
        ConfigDef.Type.BOOLEAN,
        CSFLE_ENABLED_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_ENABLED_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        CSFLE_ENABLED_DISPLAY);
    configDef.define(
        CSFLE_MASTER_KEY_PATH_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_MASTER_KEY_PATH_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_MASTER_KEY_PATH_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_MASTER_KEY_PATH_DISPLAY);
    configDef.define(
        CSFLE_DATA_KEY_NAMESPACE_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_DATA_KEY_NAMESPACE_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_DATA_KEY_NAMESPACE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_DATA_KEY_NAMESPACE_DISPLAY);
    configDef.define(
        CSFLE_CRYPT_SHARED_LIB_PATH_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_CRYPT_SHARED_LIB_PATH_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_CRYPT_SHARED_LIB_PATH_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_CRYPT_SHARED_LIB_PATH_DISPLAY);
    return configDef;
  }

  public static MongoClientSettings.Builder configureCSFLE(
      final MongoClientSettings.Builder mongoClientSettingsBuilder, final Config config) {

    boolean csfleEnabled =
        getConfigByNameWithoutErrors(config, CSFLE_ENABLED_CONFIG)
            .map(c -> (Boolean) c.value())
            .orElse(CSFLE_ENABLED_DEFAULT);

    LOGGER.info("Before going inside the csfle block  - " + csfleEnabled);

    if (csfleEnabled) {
      LOGGER.info("Moved inside the csfle block");
      String localKeyPath =
          getConfigByNameWithoutErrors(config, CSFLE_MASTER_KEY_PATH_CONFIG)
              .map(c -> (String) c.value())
              .orElse(CSFLE_MASTER_KEY_PATH_DEFAULT);

      String cryptSharedLibPath =
          getConfigByNameWithoutErrors(config, CSFLE_CRYPT_SHARED_LIB_PATH_CONFIG)
              .map(c -> (String) c.value())
              .orElse(CSFLE_CRYPT_SHARED_LIB_PATH_DEFAULT);

      String datakeyVault =
          getConfigByNameWithoutErrors(config, CSFLE_DATA_KEY_NAMESPACE_CONFIG)
              .map(c -> (String) c.value())
              .orElse(CSFLE_DATA_KEY_NAMESPACE_DEFAULT);

      return configureCSFLE(
          mongoClientSettingsBuilder, localKeyPath, datakeyVault, cryptSharedLibPath);
    }

    return mongoClientSettingsBuilder;
  }

  public static MongoClientSettings.Builder configureCSFLE(
      final MongoClientSettings.Builder mongoClientSettingsBuilder, final AbstractConfig config) {
    return configureCSFLE(
        mongoClientSettingsBuilder,
        config.getString(CSFLE_MASTER_KEY_PATH_CONFIG),
        config.getString(CSFLE_DATA_KEY_NAMESPACE_CONFIG),
        config.getString(CSFLE_CRYPT_SHARED_LIB_PATH_CONFIG));
  }

  public static MongoClientSettings.Builder configureCSFLE(
      final MongoClientSettings.Builder mongoClientSettingsBuilder,
      final String localKeyPath,
      final String dataKeyVault,
      final String cryptSharedLibPath) {

    try {
      Map<String, Map<String, Object>> kmsProviders = new HashMap<>();

      Map<String, Object> keyMap = new HashMap<String, Object>();

      if (!localKeyPath.isEmpty() && !cryptSharedLibPath.isEmpty()) {

        byte[] localMasterKeyRead = new byte[96];

        try (FileInputStream fis = new FileInputStream(localKeyPath)) {
          if (fis.read(localMasterKeyRead) < 96)
            throw new Exception("Expected to read 96 bytes from file");
        }
        keyMap.put("key", localMasterKeyRead);

        kmsProviders.put("local", keyMap);

        Map<String, Object> extraOptions = new HashMap<String, Object>();
        extraOptions.put("cryptSharedLibPath", cryptSharedLibPath);
        extraOptions.put("cryptSharedLibRequired", true);

        AutoEncryptionSettings autoEncryptionSettings =
            AutoEncryptionSettings.builder()
                .kmsProviders(kmsProviders)
                .keyVaultNamespace(dataKeyVault)
                .extraOptions(extraOptions)
                .build();

        mongoClientSettingsBuilder.autoEncryptionSettings(autoEncryptionSettings);
        LOGGER.info("Applied Encryption Settings");
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to initialize CSFLE configuration", ex);
      throw new ConfigException("Failed to connect to MongoDB with CSFLE ", ex);
    }
    return mongoClientSettingsBuilder;
  }

  private CsfleConfig() {}
}
