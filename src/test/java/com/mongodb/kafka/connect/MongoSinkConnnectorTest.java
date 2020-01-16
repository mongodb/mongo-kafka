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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTask;

@RunWith(JUnitPlatform.class)
class MongoSinkConnnectorTest {

    @Test
    @DisplayName("Should return the expected version")
    void testVersion() {
        MongoSinkConnector sinkConnector = new MongoSinkConnector();

        assertEquals(Versions.VERSION, sinkConnector.version());
    }

    @Test
    @DisplayName("test task class")
    void testTaskClass() {
        MongoSinkConnector sinkConnector = new MongoSinkConnector();

        assertEquals(MongoSinkTask.class, sinkConnector.taskClass());
    }

    @Test
    @DisplayName("test task configs")
    void testConfig() {
        MongoSinkConnector sinkConnector = new MongoSinkConnector();

        assertEquals(MongoSinkConfig.CONFIG, sinkConnector.config());
    }

    @Test
    @DisplayName("test task configs")
    void testTaskConfigs() {
        MongoSinkConnector sinkConnector = new MongoSinkConnector();
        Map<String, String> configMap = new HashMap<String, String>() {{
            put("a", "1");
            put("b", "2");
        }};
        sinkConnector.start(configMap);
        List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(100);

        assertEquals(1, taskConfigs.size());
        assertEquals(configMap, taskConfigs.get(0));
    }
}
