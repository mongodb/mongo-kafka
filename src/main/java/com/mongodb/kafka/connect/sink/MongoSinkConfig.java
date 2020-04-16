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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.util.Validators.errorCheckingValueValidator;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
import static org.apache.kafka.common.config.ConfigDef.Width;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import com.mongodb.ConnectionString;

import com.mongodb.kafka.connect.MongoSinkConnector;
import com.mongodb.kafka.connect.util.Validators;

public class MongoSinkConfig extends AbstractConfig {

    public static final String TOPICS_CONFIG = MongoSinkConnector.TOPICS_CONFIG;
    private static final String TOPICS_DISPLAY = "The Kafka topics";
    private static final String TOPICS_DOC = "A list of kafka topics for the sink connector.";

    public static final String CONNECTION_URI_CONFIG = "connection.uri";
    private static final String CONNECTION_URI_DEFAULT = "mongodb://localhost:27017";
    private static final String CONNECTION_URI_DISPLAY = "MongoDB Connection URI";
    private static final String CONNECTION_URI_DOC = "The connection URI as supported by the official drivers. "
            + "eg: ``mongodb://user@pass@locahost/``.";

    public static final String TOPIC_OVERRIDE_CONFIG = "topic.override.%s.%s";
    private static final String TOPIC_OVERRIDE_DEFAULT = "";
    private static final String TOPIC_OVERRIDE_DISPLAY = "Per topic configuration overrides.";
    public static final String TOPIC_OVERRIDE_DOC = "The overrides configuration allows for per topic customization of configuration. "
            + "The customized overrides are merged with the default configuration, to create the specific configuration for a topic.\n"
            + "For example, ``topic.override.foo.collection=bar`` will store data from the ``foo`` topic into the ``bar`` collection.\n"
            + "Note: All configuration options apart from '" + CONNECTION_URI_CONFIG + "' and '" + TOPICS_CONFIG + "' are overridable.";

    private Map<String, String> originals;
    private final List<String> topics;
    private Map<String, MongoSinkTopicConfig> topicSinkConnectorConfigMap;
    private ConnectionString connectionString;

    public MongoSinkConfig(final Map<String, String> originals) {
        super(CONFIG, originals, false);
        this.originals = unmodifiableMap(originals);
        topics = unmodifiableList(getList(TOPICS_CONFIG));
        connectionString = new ConnectionString(getString(CONNECTION_URI_CONFIG));

        topics.forEach(this::getMongoSinkTopicConfig);
    }

    public static final ConfigDef CONFIG = createConfigDef();

    public static String createOverrideKey(final String topic, final String config) {
        if (!CONFIG.configKeys().containsKey(config)) {
            throw new ConfigException("Unknown configuration key: " + config);
        }
        return format(TOPIC_OVERRIDE_CONFIG, topic, config);
    }

    public ConnectionString getConnectionString() {
        return connectionString;
    }

    public List<String> getTopics() {
        return topics;
    }

    public MongoSinkTopicConfig getMongoSinkTopicConfig(final String topic) {
        if (!topics.contains(topic)) {
            throw new IllegalArgumentException(format("Unknown topic: %s, must be one of: %s", topic, topics));
        }
        if (topicSinkConnectorConfigMap == null) {
            createMongoSinkTopicConfig();
        }
        return topicSinkConnectorConfigMap.get(topic);
    }

    private void createMongoSinkTopicConfig() {
        topicSinkConnectorConfigMap = topics.stream()
                .collect(Collectors.toMap((t) -> t, (t) -> new MongoSinkTopicConfig(t, originals)));
    }

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef() {

            @Override
            @SuppressWarnings("unchecked")
            public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
                Map<String, ConfigValue> results = super.validateAll(props);
                // Don't validate child configs if the top level configs are broken
                if (results.values().stream().anyMatch((c) -> !c.errorMessages().isEmpty())) {
                    return results;
                }

                // Validate any topic based configs
                List<String> topics = (List<String>) results.get(TOPICS_CONFIG).value();
                topics.forEach(topic -> results.putAll(MongoSinkTopicConfig.validateAll(topic, props)));
                return results;
            }
        };
        String group = "Connection";
        int orderInGroup = 0;
        configDef.define(TOPICS_CONFIG,
                Type.LIST,
                NO_DEFAULT_VALUE,
                Validators.nonEmptyList(),
                Importance.HIGH,
                TOPICS_DOC,
                group,
                ++orderInGroup,
                Width.MEDIUM,
                TOPICS_DISPLAY);
        configDef.define(CONNECTION_URI_CONFIG,
                Type.STRING,
                CONNECTION_URI_DEFAULT,
                errorCheckingValueValidator("A valid connection string", ConnectionString::new),
                Importance.HIGH,
                CONNECTION_URI_DOC,
                group,
                ++orderInGroup,
                Width.MEDIUM,
                CONNECTION_URI_DISPLAY);

        group = "Overrides";
        orderInGroup = 0;
        configDef.define(TOPIC_OVERRIDE_CONFIG,
                Type.STRING,
                TOPIC_OVERRIDE_DEFAULT,
                Validators.topicOverrideValidator(),
                Importance.LOW,
                TOPIC_OVERRIDE_DOC,
                group,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_OVERRIDE_DISPLAY);

        MongoSinkTopicConfig.BASE_CONFIG.configKeys().values().forEach(configDef::define);
        return configDef;
    }

}
