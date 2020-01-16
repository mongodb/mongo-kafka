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

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceTask;

@RunWith(JUnitPlatform.class)
class MongoSourceConnnectorTest {

    @Test
    @DisplayName("Should return the expected version")
    void testVersion() {
        MongoSourceConnector sourceConnector = new MongoSourceConnector();

        assertEquals(Versions.VERSION, sourceConnector.version());
    }

    @Test
    @DisplayName("test task class")
    void testTaskClass() {
        MongoSourceConnector sourceConnector = new MongoSourceConnector();

        assertEquals(MongoSourceTask.class, sourceConnector.taskClass());
    }

    @Test
    @DisplayName("test task configs")
    void testConfig() {
        MongoSourceConnector sourceConnector = new MongoSourceConnector();

        assertEquals(MongoSourceConfig.CONFIG, sourceConnector.config());
    }

    @Test
    @DisplayName("test task configs")
    void testTaskConfigs() {
        MongoSourceConnector sourceConnector = new MongoSourceConnector();
        Map<String, String> configMap = new HashMap<String, String>() {{
            put("a", "1");
            put("b", "2");
        }};
        sourceConnector.start(configMap);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(100);

        assertEquals(1, taskConfigs.size());
        assertEquals(configMap, taskConfigs.get(0));
    }
}
