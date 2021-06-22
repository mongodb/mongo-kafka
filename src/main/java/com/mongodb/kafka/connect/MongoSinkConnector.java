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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.util.ConfigHelper.getConfigByName;
import static com.mongodb.kafka.connect.util.ConnectionValidator.validateCanConnect;
import static com.mongodb.kafka.connect.util.ConnectionValidator.validateUserHasActions;
import static com.mongodb.kafka.connect.util.TimeseriesValidation.validTopicRegexConfigAndCollection;
import static com.mongodb.kafka.connect.util.TimeseriesValidation.validateConfigAndCollection;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTask;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

public class MongoSinkConnector extends SinkConnector {
  private static final List<String> REQUIRED_SINK_ACTIONS = asList("insert", "update", "remove");
  private static final List<String> REQUIRED_COLLSTATS_SINK_ACTIONS =
      asList("insert", "update", "remove", "collStats");
  private Map<String, String> settings;

  @Override
  public String version() {
    return Versions.VERSION;
  }

  @Override
  public void start(final Map<String, String> map) {
    settings = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MongoSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(final int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(settings);
    }
    return configs;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return MongoSinkConfig.CONFIG;
  }

  @Override
  public Config validate(final Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);

    MongoSinkConfig sinkConfig;
    try {
      sinkConfig = new MongoSinkConfig(connectorConfigs);
    } catch (Exception e) {
      return config;
    }

    validateCanConnect(config, MongoSinkConfig.CONNECTION_URI_CONFIG)
        .ifPresent(
            client -> {
              try {
                sinkConfig
                    .getTopics()
                    .ifPresent(
                        topics ->
                            topics.forEach(
                                topic -> {
                                  MongoSinkTopicConfig mongoSinkTopicConfig =
                                      sinkConfig.getMongoSinkTopicConfig(topic);
                                  validateUserHasActions(
                                      client,
                                      sinkConfig.getConnectionString().getCredential(),
                                      mongoSinkTopicConfig.isTimeseries()
                                          ? REQUIRED_COLLSTATS_SINK_ACTIONS
                                          : REQUIRED_SINK_ACTIONS,
                                      mongoSinkTopicConfig.getString(
                                          MongoSinkTopicConfig.DATABASE_CONFIG),
                                      mongoSinkTopicConfig.getString(
                                          MongoSinkTopicConfig.COLLECTION_CONFIG),
                                      MongoSinkConfig.CONNECTION_URI_CONFIG,
                                      config);
                                  validateConfigAndCollection(client, mongoSinkTopicConfig, config);
                                }));
                sinkConfig
                    .getTopicRegex()
                    .ifPresent(
                        regex -> {
                          validateUserHasActions(
                              client,
                              sinkConfig.getConnectionString().getCredential(),
                              REQUIRED_SINK_ACTIONS,
                              getConfigByName(config, MongoSinkTopicConfig.DATABASE_CONFIG)
                                  .map(c -> (String) c.value())
                                  .orElse(""),
                              getConfigByName(config, MongoSinkTopicConfig.COLLECTION_CONFIG)
                                  .map(c -> (String) c.value())
                                  .orElse(""),
                              MongoSinkConfig.CONNECTION_URI_CONFIG,
                              config);
                          validTopicRegexConfigAndCollection(client, sinkConfig, config);
                        });
              } catch (Exception e) {
                // Ignore
              } finally {
                client.close();
              }
            });

    return config;
  }
}
