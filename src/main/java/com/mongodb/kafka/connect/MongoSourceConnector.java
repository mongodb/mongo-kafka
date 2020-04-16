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
import org.apache.kafka.connect.source.SourceConnector;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceTask;

public class MongoSourceConnector extends SourceConnector {
    private static final List<String> REQUIRED_SOURCE_ACTIONS = asList("changeStream", "find");
    private Map<String, String> settings;

    @Override
    public void start(final Map<String, String> props) {
        settings = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoSourceTask.class;
    }

    @Override
    public Config validate(final Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        MongoSourceConfig sourceConfig;
        try {
            sourceConfig = new MongoSourceConfig(connectorConfigs);
        } catch (Exception e) {
            return config;
        }

        validateCanConnect(config, MongoSourceConfig.CONNECTION_URI_CONFIG)
                .ifPresent(client -> {
                    try {
                        validateUserHasActions(client,
                                sourceConfig.getConnectionString().getCredential(),
                                REQUIRED_SOURCE_ACTIONS,
                                sourceConfig.getString(MongoSourceConfig.DATABASE_CONFIG),
                                sourceConfig.getString(MongoSourceConfig.COLLECTION_CONFIG),
                                MongoSourceConfig.CONNECTION_URI_CONFIG, config);
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        client.close();
                    }
                });

        return config;
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
        return MongoSourceConfig.CONFIG;
    }

    @Override
    public String version() {
        return Versions.VERSION;
    }
}
