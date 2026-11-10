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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceTask;
import com.mongodb.kafka.connect.util.ConfigHelper;
import com.mongodb.kafka.connect.util.ConnectionUriBlocklist;

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
    Map<String, String> configMap =
        new HashMap<String, String>() {
          {
            put("a", "1");
            put("b", "2");
          }
        };
    sourceConnector.start(configMap);
    List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(100);

    assertEquals(1, taskConfigs.size());
    assertEquals(configMap, taskConfigs.get(0));
  }

  @Test
  @DisplayName("Should not expose resolved secrets in the validate() response for invalid configs")
  void testValidateMasksResolvedSecrets() {
    MongoSourceConnector sourceConnector = new MongoSourceConnector();

    String secret = "user=alice password=super-secret-hunter2";
    Map<String, String> configs = new HashMap<>();
    configs.put(CONNECTION_URI_CONFIG, secret);

    Config config = sourceConnector.validate(configs);

    ValidateAssertions.assertSecretAbsent(config, secret);

    // The underlying failure is still reported, just without the secret in it.
    Optional<ConfigValue> uri = ConfigHelper.getConfigByName(config, CONNECTION_URI_CONFIG);
    assertTrue(uri.isPresent());
    assertFalse(
        uri.get().errorMessages().isEmpty(), "connection.uri should still be reported as invalid");
  }

  @Test
  @DisplayName("Should not flag a valid connection.uri when an unrelated config is invalid")
  void testValidateDoesNotFlagValidConnectionUri() {
    MongoSourceConnector sourceConnector = new MongoSourceConnector();

    String uri = "mongodb://user:uri-secret-pw@localhost:27017/?connectTimeoutMS=300";
    Map<String, String> configs = new HashMap<>();
    configs.put(CONNECTION_URI_CONFIG, uri);
    configs.put(HEARTBEAT_INTERVAL_MS_CONFIG, "not-a-number");

    Config config = sourceConnector.validate(configs);

    Optional<ConfigValue> uriValue = ConfigHelper.getConfigByName(config, CONNECTION_URI_CONFIG);
    assertTrue(uriValue.isPresent());
    assertTrue(
        uriValue.get().errorMessages().isEmpty(),
        "valid connection.uri must not be reported as invalid: " + uriValue.get().errorMessages());

    ValidateAssertions.assertSecretAbsent(config, "uri-secret-pw");

    // The unrelated failure is still surfaced.
    Optional<ConfigValue> heartbeatValue =
        ConfigHelper.getConfigByName(config, HEARTBEAT_INTERVAL_MS_CONFIG);
    assertTrue(heartbeatValue.isPresent());
    assertFalse(heartbeatValue.get().errorMessages().isEmpty());
  }

  @Test
  @DisplayName("Should reject blocked connection.uri options in the validate() response")
  void testValidateRejectsBlockedConnectionUriOptions() {
    MongoSourceConnector sourceConnector = new MongoSourceConnector();

    String uri = "mongodb://user:uri-secret-pw@fake-mongod:27017/?authMechanism=MONGODB-AWS";
    Map<String, String> configs = new HashMap<>();
    configs.put(CONNECTION_URI_CONFIG, uri);

    Config config = sourceConnector.validate(configs);

    ValidateAssertions.assertSecretAbsent(config, "uri-secret-pw");

    Optional<ConfigValue> uriValue = ConfigHelper.getConfigByName(config, CONNECTION_URI_CONFIG);
    assertTrue(uriValue.isPresent());
    assertFalse(
        uriValue.get().errorMessages().isEmpty(),
        "blocked connection.uri options should be reported as invalid");
    assertTrue(
        uriValue.get().errorMessages().get(0).contains("authmechanism"),
        "expected the blocked option to be named, got: " + uriValue.get().errorMessages().get(0));
  }

  @Test
  @DisplayName("Should allow connection.uri options removed from the blocklist")
  void testValidateAllowsOptionsRemovedFromBlocklist() {
    MongoSourceConnector sourceConnector = new MongoSourceConnector();

    // tlsInsecure avoids an ambient AWS credential lookup the driver would perform for
    // MONGODB-AWS during the connection attempt this test makes it to
    Map<String, String> configs = new HashMap<>();
    configs.put(CONNECTION_URI_CONFIG, "mongodb://localhost:27017/?tlsInsecure=true");
    configs.put(ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG, "");
    configs.put(HEARTBEAT_INTERVAL_MS_CONFIG, "not-a-number");

    Config config = sourceConnector.validate(configs);

    Optional<ConfigValue> uriValue = ConfigHelper.getConfigByName(config, CONNECTION_URI_CONFIG);
    assertTrue(uriValue.isPresent());
    assertTrue(
        uriValue.get().errorMessages().stream()
            .noneMatch(m -> m.contains("disallowed connection string options")),
        "connection.uri must not be reported as blocked: " + uriValue.get().errorMessages());
  }
}
