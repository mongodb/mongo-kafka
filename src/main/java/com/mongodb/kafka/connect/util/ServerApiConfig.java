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

import java.util.Optional;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;

public final class ServerApiConfig {

  private static final String EMPTY_STRING = "";

  public static final String SERVER_API_VERSION_CONFIG = "server.api.version";
  private static final String SERVER_API_VERSION_DEFAULT = EMPTY_STRING;
  private static final String SERVER_API_VERSION_DISPLAY = "The server API version.";
  private static final String SERVER_API_VERSION_DOC =
      "The server API version to use. Disabled by default.";

  public static final String SERVER_API_DEPRECATION_ERRORS_CONFIG = "server.api.deprecation.errors";
  private static final boolean SERVER_API_DEPRECATION_ERRORS_DEFAULT = false;
  private static final String SERVER_API_DEPRECATION_ERRORS_DISPLAY = "Deprecation errors";
  private static final String SERVER_API_DEPRECATION_ERRORS_DOC =
      "Sets whether the connector requires use of deprecated server APIs to be reported as errors.";

  public static final String SERVER_API_STRICT_CONFIG = "server.api.strict";
  private static final boolean SERVER_API_STRICT_DEFAULT = false;
  private static final String SERVER_API_STRICT_DISPLAY = "Strict";
  private static final String SERVER_API_STRICT_DOC =
      "Sets whether the application requires strict server API version enforcement.";

  public static ConfigDef addServerApiConfig(final ConfigDef configDef) {
    String group = "Server Api";
    int orderInGroup = 0;
    configDef.define(
        SERVER_API_VERSION_CONFIG,
        ConfigDef.Type.STRING,
        SERVER_API_VERSION_DEFAULT,
        Validators.emptyString()
            .or(
                Validators.errorCheckingValueValidator(
                    "A valid server version", ServerApiVersion::findByValue)),
        ConfigDef.Importance.MEDIUM,
        SERVER_API_VERSION_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        SERVER_API_VERSION_DISPLAY);
    configDef.define(
        SERVER_API_DEPRECATION_ERRORS_CONFIG,
        ConfigDef.Type.BOOLEAN,
        SERVER_API_DEPRECATION_ERRORS_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        SERVER_API_DEPRECATION_ERRORS_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        SERVER_API_DEPRECATION_ERRORS_DISPLAY);
    configDef.define(
        SERVER_API_STRICT_CONFIG,
        ConfigDef.Type.BOOLEAN,
        SERVER_API_STRICT_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        SERVER_API_STRICT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        SERVER_API_STRICT_DISPLAY);

    return configDef;
  }

  public static MongoClientSettings.Builder setServerApi(
      final MongoClientSettings.Builder mongoClientSettingsBuilder, final Config config) {
    Optional<ConfigValue> serverApiVersionConfig =
        getConfigByNameWithoutErrors(config, SERVER_API_VERSION_CONFIG);
    if (serverApiVersionConfig.isPresent()
        && serverApiVersionConfig.get().errorMessages().isEmpty()) {
      String serverApiVersion =
          serverApiVersionConfig.map(c -> (String) c.value()).orElse(SERVER_API_VERSION_DEFAULT);
      boolean deprecationErrors =
          getConfigByNameWithoutErrors(config, SERVER_API_DEPRECATION_ERRORS_CONFIG)
              .map(c -> (Boolean) c.value())
              .orElse(SERVER_API_DEPRECATION_ERRORS_DEFAULT);
      boolean strict =
          getConfigByNameWithoutErrors(config, SERVER_API_STRICT_CONFIG)
              .map(c -> (Boolean) c.value())
              .orElse(SERVER_API_STRICT_DEFAULT);
      setServerApi(mongoClientSettingsBuilder, serverApiVersion, deprecationErrors, strict);
    }
    return mongoClientSettingsBuilder;
  }

  public static MongoClientSettings.Builder setServerApi(
      final MongoClientSettings.Builder mongoClientSettingsBuilder, final AbstractConfig config) {
    return setServerApi(
        mongoClientSettingsBuilder,
        config.getString(SERVER_API_VERSION_CONFIG),
        config.getBoolean(SERVER_API_DEPRECATION_ERRORS_CONFIG),
        config.getBoolean(SERVER_API_STRICT_CONFIG));
  }

  private static MongoClientSettings.Builder setServerApi(
      final MongoClientSettings.Builder mongoClientSettingsBuilder,
      final String serverApiVersion,
      final boolean deprecationErrors,
      final boolean strict) {
    if (!serverApiVersion.isEmpty()) {
      ServerApi.Builder serverApiBuilder = ServerApi.builder();
      serverApiBuilder.version(ServerApiVersion.findByValue(serverApiVersion));
      serverApiBuilder.deprecationErrors(deprecationErrors);
      serverApiBuilder.strict(strict);
      mongoClientSettingsBuilder.serverApi(serverApiBuilder.build());
    }
    return mongoClientSettingsBuilder;
  }

  private ServerApiConfig() {}
}
