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

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_REGEX_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTask;
import com.mongodb.kafka.connect.util.ConfigHelper;

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
    Map<String, String> configMap =
        new HashMap<String, String>() {
          {
            put("a", "1");
            put("b", "2");
          }
        };
    sinkConnector.start(configMap);
    List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(10);

    assertEquals(10, taskConfigs.size());
    IntStream.range(0, 10).boxed().forEach(i -> assertEquals(configMap, taskConfigs.get(1)));
  }

  @Test
  @DisplayName("Should not expose resolved secrets in the validate() response for invalid configs")
  void testValidateMasksResolvedSecrets() {
    MongoSinkConnector sinkConnector = new MongoSinkConnector();

    String secret = "user=alice password=super-secret-hunter2";
    Map<String, String> configs = new HashMap<>();
    configs.put(CONNECTION_URI_CONFIG, secret);
    configs.put(TOPICS_CONFIG, "orders");

    Config config = sinkConnector.validate(configs);

    ValidateAssertions.assertSecretAbsent(config, secret);

    // The underlying failure is still reported, just without the secret in it.
    Optional<ConfigValue> uri = ConfigHelper.getConfigByName(config, CONNECTION_URI_CONFIG);
    assertTrue(uri.isPresent());
    assertFalse(
        uri.get().errorMessages().isEmpty(), "connection.uri should still be reported as invalid");
    assertTrue(
        uri.get().errorMessages().get(0).contains("[hidden]"),
        "error message should show the masked placeholder, got: " + uri.get().errorMessages());
  }

  @Test
  @DisplayName("Should not flag a valid connection.uri when an unrelated config is invalid")
  void testValidateDoesNotFlagValidConnectionUri() {
    MongoSinkConnector sinkConnector = new MongoSinkConnector();

    // Valid connection string (with a credential); an unrelated config (invalid regex) fails
    // construction. The valid URI must not be falsely reported as invalid.
    String uri = "mongodb://user:uri-secret-pw@localhost:27017/?connectTimeoutMS=300";
    Map<String, String> configs = new HashMap<>();
    configs.put(CONNECTION_URI_CONFIG, uri);
    configs.put(TOPICS_REGEX_CONFIG, "[");

    Config config = sinkConnector.validate(configs);

    Optional<ConfigValue> uriValue = ConfigHelper.getConfigByName(config, CONNECTION_URI_CONFIG);
    assertTrue(uriValue.isPresent());
    assertTrue(
        uriValue.get().errorMessages().isEmpty(),
        "valid connection.uri must not be reported as invalid: " + uriValue.get().errorMessages());

    ValidateAssertions.assertSecretAbsent(config, "uri-secret-pw");

    // The unrelated failure is still surfaced.
    Optional<ConfigValue> regexValue = ConfigHelper.getConfigByName(config, TOPICS_REGEX_CONFIG);
    assertTrue(regexValue.isPresent());
    assertFalse(regexValue.get().errorMessages().isEmpty());
    assertTrue(
        regexValue.get().errorMessages().get(0).contains("Invalid regex"),
        "expected a regex validation error, got: " + regexValue.get().errorMessages());
  }
}
