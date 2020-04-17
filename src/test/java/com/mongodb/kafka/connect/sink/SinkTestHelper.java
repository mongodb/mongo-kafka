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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_REGEX_CONFIG;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

public final class SinkTestHelper {

    public static final String CLIENT_URI_DEFAULT_SETTINGS = "mongodb://localhost:27017";
    public static final String CLIENT_URI_AUTH_SETTINGS = "mongodb://user:pass@localhost:27017/kafkaconnect";
    public static final String TEST_TOPIC = "topic";
    public static final String TEST_DATABASE = "myDB";

    public static Map<String, String> createConfigMap() {
        Map<String, String> map = new HashMap<>();
        map.put(TOPICS_CONFIG, TEST_TOPIC);
        map.put(DATABASE_CONFIG, TEST_DATABASE);
        return map;
    }


    public static  Map<String, String> createConfigMap(final String json) {
        Map<String, String> map = createConfigMap();
        Document.parse(json).forEach((k, v) -> {
            if (k.equals(TOPICS_REGEX_CONFIG)) {
                map.remove(TOPICS_CONFIG);
            }
            map.put(k, v.toString());
        });
        return map;
    }

    public static Map<String, String> createConfigMap(final String k, final String v) {
        Map<String, String> map = createConfigMap();
        if (k.equals(TOPICS_REGEX_CONFIG)) {
            map.remove(TOPICS_CONFIG);
        }
        map.put(k, v);
        return map;
    }

    public static MongoSinkConfig createSinkConfig() {
        return new MongoSinkConfig(createConfigMap());
    }

    public static MongoSinkConfig createSinkConfig(final String json) {
        return new MongoSinkConfig(createConfigMap(json));
    }

    public static MongoSinkConfig createSinkConfig(final String k, final String v) {
        return new MongoSinkConfig(createConfigMap(k, v));
    }

    public static MongoSinkTopicConfig createTopicConfig() {
        return createSinkConfig().getMongoSinkTopicConfig(TEST_TOPIC);
    }

    public static MongoSinkTopicConfig createTopicConfig(final String k, final String v) {
        return createSinkConfig(k, v).getMongoSinkTopicConfig(TEST_TOPIC);
    }

    public static MongoSinkTopicConfig createTopicConfig(final String json) {
        return createSinkConfig(json).getMongoSinkTopicConfig(TEST_TOPIC);
    }

    private SinkTestHelper(){
    }
}
