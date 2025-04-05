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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

  public static final String CSFLE_LOCAL_MASTER_KEY_PATH_CONFIG = "csfle.master.key";
  private static final String CSFLE_MASTER_KEY_PATH_DEFAULT = "/etc/ssl/localKey";
  private static final String CSFLE_MASTER_KEY_PATH_DISPLAY = "Local Key Path";
  private static final String CSFLE_MASTER_KEY_PATH_DOC =
      "Specifies the master key path used for encryption.";

  public static final String CSFLE_AWS_ACCESS_KEY_CONFIG = "csfle.aws.access.key";
  private static final String CSFLE_AWS_ACCESS_KEY_DEFAULT = "";
  private static final String CSFLE_AWS_ACCESS_KEY_DISPLAY = "AWS Access Key";
  private static final String CSFLE_AWS_ACCESS_KEY_DOC =
      "Specifies the aws access key used for encryption.";

  public static final String CSFLE_AWS_SECRET_ACCESS_KEY_CONFIG = "csfle.aws.secret.key";
  private static final String CSFLE_AWS_SECRET_ACCESS_KEY_DEFAULT = "";
  private static final String CSFLE_AWS_SECRET_ACCESS_KEY_DISPLAY = "AWS Secret Access Key";
  private static final String CSFLE_AWS_SECRET_ACCESS_KEY_DOC =
      "Specifies the aws secret access key";

  public static final String CSFLE_AZURE_TENANT_ID_CONFIG = "csfle.azure.tenant.id";
  private static final String CSFLE_AZURE_TENANT_ID_DEFAULT = "";
  private static final String CSFLE_AZURE_TENANT_ID_DISPLAY = "Azure Tenant Id";
  private static final String CSFLE_AZURE_TENANT_ID_DOC = "Specifies the azure tenantId";

  public static final String CSFLE_AZURE_CLIENT_ID_CONFIG = "csfle.azure.client.id";
  private static final String CSFLE_AZURE_CLIENT_ID_DEFAULT = "";
  private static final String CSFLE_AZURE_CLIENT_ID_DISPLAY = "Azure ClientId";
  private static final String CSFLE_AZURE_CLIENT_ID_DOC = "Specifies the azure clientId";

  public static final String CSFLE_AZURE_CLIENT_SECRET_CONFIG = "csfle.azure.clientSecret";
  private static final String CSFLE_AZURE_CLIENT_SECRET_DEFAULT = "";
  private static final String CSFLE_AZURE_CLIENT_SECRET_DISPLAY = "Azure Client Secret";
  private static final String CSFLE_AZURE_CLIENT_SECRET_DOC = "Specifies the azure client secret";

  public static final String CSFLE_KMIP_ENDPOINT_CONFIG = "csfle.kmip.endpoint";
  private static final String CSFLE_KMIP_ENDPOINT_DEFAULT = "";
  private static final String CSFLE_KMIP_ENDPOINT_DISPLAY = "KMIP Endpoint";
  private static final String CSFLE_KMIP_ENDPOINT_DOC = "Specifies the azure client secret";

  public static final String CSFLE_CRYPT_SHARED_LIB_PATH_CONFIG = "csfle.cryptSharedLib.path";
  private static final String CSFLE_CRYPT_SHARED_LIB_PATH_DEFAULT = "/etc/ssl/mongo_crypt_v1.so";
  private static final String CSFLE_CRYPT_SHARED_LIB_PATH_DISPLAY = "CryptSharedLib Path";
  private static final String CSFLE_CRYPT_SHARED_LIB_PATH_DOC =
      "Crypt Shared Lib Path for automatic encryption";

  public static final String CSFLE_DATA_KEY_NAMESPACE_CONFIG = "csfle.datakey.path";
  private static final String CSFLE_DATA_KEY_NAMESPACE_DEFAULT = "encryption._keyvault";
  private static final String CSFLE_DATA_KEY_NAMESPACE_DISPLAY = "Data Key Vault Namespace";
  private static final String CSFLE_DATA_KEY_NAMESPACE_DOC = "Data key Vault Namespace for CSFLE";

  public static final String CSFLE_KMS_PROVIDER_CONFIG = "csfle.kms.provider";
  private static final String CSFLE_KMS_PROVIDER_DEFAULT = KMSProviders.LOCAL.getName();
  private static final String CSFLE_KMS_PROVIDER_DISPLAY = "KMS Provider";
  private static final String CSFLE_KMS_PROVIDER_DOC =
      "Valid value of KMS Provider, eg - local, aws, azure, kmip";

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
        CSFLE_KMS_PROVIDER_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_KMS_PROVIDER_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_KMS_PROVIDER_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        CSFLE_KMS_PROVIDER_DISPLAY,
        Arrays.asList(
            CSFLE_LOCAL_MASTER_KEY_PATH_CONFIG,
            CSFLE_AWS_ACCESS_KEY_CONFIG,
            CSFLE_AWS_SECRET_ACCESS_KEY_CONFIG,
            CSFLE_AZURE_CLIENT_ID_CONFIG,
            CSFLE_AZURE_TENANT_ID_CONFIG,
            CSFLE_AZURE_CLIENT_SECRET_CONFIG,
            CSFLE_KMIP_ENDPOINT_CONFIG),
        new KMSProviderRecommender(
            Arrays.asList(
                KMSProviders.LOCAL, KMSProviders.AZURE, KMSProviders.AWS, KMSProviders.KMIP)));
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

    group = "CSFLE Provider Details";
    orderInGroup = 0;

    /*
    LOCAL KMS
     */
    configDef.define(
        CSFLE_LOCAL_MASTER_KEY_PATH_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_MASTER_KEY_PATH_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_MASTER_KEY_PATH_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_MASTER_KEY_PATH_DISPLAY,
        new KMSProviderMapper(KMSProviders.LOCAL));

    /*
    AWS KMS
     */

    configDef.define(
        CSFLE_AWS_ACCESS_KEY_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_AWS_ACCESS_KEY_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_AWS_ACCESS_KEY_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_AWS_ACCESS_KEY_DISPLAY,
        new KMSProviderMapper(KMSProviders.AWS));

    configDef.define(
        CSFLE_AWS_SECRET_ACCESS_KEY_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_AWS_SECRET_ACCESS_KEY_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_AWS_SECRET_ACCESS_KEY_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_AWS_SECRET_ACCESS_KEY_DISPLAY,
        new KMSProviderMapper(KMSProviders.AWS));

    /*
    AZURE KMS
     */
    configDef.define(
        CSFLE_AZURE_TENANT_ID_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_AZURE_TENANT_ID_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_AZURE_TENANT_ID_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_AZURE_TENANT_ID_DISPLAY,
        new KMSProviderMapper(KMSProviders.AZURE));

    configDef.define(
        CSFLE_AZURE_CLIENT_SECRET_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_AZURE_CLIENT_SECRET_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_AZURE_CLIENT_SECRET_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_AZURE_CLIENT_SECRET_DISPLAY,
        new KMSProviderMapper(KMSProviders.AZURE));

    configDef.define(
        CSFLE_AZURE_CLIENT_ID_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_AZURE_CLIENT_ID_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_AZURE_CLIENT_ID_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_AZURE_CLIENT_ID_DISPLAY,
        new KMSProviderMapper(KMSProviders.AZURE));

    /*
    KMIP
     */
    configDef.define(
        CSFLE_KMIP_ENDPOINT_CONFIG,
        ConfigDef.Type.STRING,
        CSFLE_KMIP_ENDPOINT_DEFAULT,
        ConfigDef.Importance.HIGH,
        CSFLE_KMIP_ENDPOINT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        CSFLE_KMIP_ENDPOINT_DISPLAY,
        new KMSProviderMapper(KMSProviders.KMIP));

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
          getConfigByNameWithoutErrors(config, CSFLE_LOCAL_MASTER_KEY_PATH_CONFIG)
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

  public static class KMSProviderRecommender implements ConfigDef.Recommender {

    private List<Object> values;

    public KMSProviderRecommender(final List<Object> values) {
      this.values = values;
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
      return values;
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {

      return true;
    }
  }

  public static MongoClientSettings.Builder configureCSFLE(
      final MongoClientSettings.Builder mongoClientSettingsBuilder, final AbstractConfig config) {
    return configureCSFLE(
        mongoClientSettingsBuilder,
        config.getString(CSFLE_LOCAL_MASTER_KEY_PATH_CONFIG),
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
          if (fis.read(localMasterKeyRead) < 96) {
            throw new Exception("Expected to read 96 bytes from file");
          }
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

  public enum KMSProviders {
    LOCAL("LOCAL"),
    AWS("AWS"),
    AZURE("AZURE"),
    KMIP("KMIP");

    private final String name;

    KMSProviders(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public static class KMSProviderMapper implements ConfigDef.Recommender {

    private KMSProviders provider;

    public KMSProviderMapper(final KMSProviders provider) {
      this.provider = provider;
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
      return Collections.emptyList();
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
      String kmsProvider = (String) parsedConfig.get("csfle.kms.provider");
      return kmsProvider.equals(provider.getName());
    }
  }

  private CsfleConfig() {}
}
