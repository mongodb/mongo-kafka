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

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getConfigByName;
import static com.mongodb.kafka.connect.util.ConfigHelper.getConfigByNameWithoutErrors;
import static com.mongodb.kafka.connect.util.MongoClientHelper.isAtleastFiveDotZero;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;

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

  public static void validateServerApi(final MongoClient mongoClient, final Config config) {
    getConfigByName(config, SERVER_API_VERSION_CONFIG)
        .ifPresent(
            serverApiVersion -> {
              if (!SERVER_API_VERSION_DEFAULT.equals(serverApiVersion.value())
                  && !isAtleastFiveDotZero(mongoClient)) {
                getConfigByName(config, CONNECTION_URI_CONFIG)
                    .ifPresent(
                        c ->
                            c.addErrorMessage(
                                "Server Version API requires MongoDB 5.0 or greater"));
              }
            });
  }

  public static MongoClientSettings.Builder setServerApi(
      final MongoClientSettings.Builder mongoClientSettingsBuilder, final Config config) {
    getConfigByNameWithoutErrors(config, SERVER_API_VERSION_CONFIG)
        .filter(s -> s.errorMessages().isEmpty())
        .ifPresent(
            serverApiVersionObject -> {
              String serverApiVersion = (String) serverApiVersionObject.value();
              boolean deprecationErrors =
                  getConfigByNameWithoutErrors(config, SERVER_API_DEPRECATION_ERRORS_CONFIG)
                      .map(c -> (Boolean) c.value())
                      .orElse(SERVER_API_DEPRECATION_ERRORS_DEFAULT);
              boolean strict =
                  getConfigByNameWithoutErrors(config, SERVER_API_STRICT_CONFIG)
                      .map(c -> (Boolean) c.value())
                      .orElse(SERVER_API_STRICT_DEFAULT);
              setServerApi(mongoClientSettingsBuilder, serverApiVersion, deprecationErrors, strict);
            });
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
