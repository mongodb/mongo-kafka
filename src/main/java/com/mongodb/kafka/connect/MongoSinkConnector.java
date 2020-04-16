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

import static com.mongodb.kafka.connect.util.ConnectionValidator.validateCanConnect;
import static com.mongodb.kafka.connect.util.ConnectionValidator.validateUserHasActions;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTask;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;

public class MongoSinkConnector extends SinkConnector {
    private static final List<String> REQUIRED_SINK_ACTIONS = asList("insert", "update", "remove");
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
        return singletonList(settings);
    }

    @Override
    public void stop() {
    }

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
                .ifPresent(client -> {
                    try {
                        sinkConfig.getTopics().forEach(topic -> {
                            MongoSinkTopicConfig mongoSinkTopicConfig = sinkConfig.getMongoSinkTopicConfig(topic);
                            validateUserHasActions(client,
                                    sinkConfig.getConnectionString().getCredential(),
                                    REQUIRED_SINK_ACTIONS,
                                    mongoSinkTopicConfig.getString(MongoSourceConfig.DATABASE_CONFIG),
                                    mongoSinkTopicConfig.getString(MongoSourceConfig.COLLECTION_CONFIG),
                                    MongoSourceConfig.CONNECTION_URI_CONFIG, config);

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
