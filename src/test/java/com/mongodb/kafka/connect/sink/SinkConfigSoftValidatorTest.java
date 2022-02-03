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

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.createOverrideKey;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DELETE_ON_NULL_VALUES_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DELETE_ON_NULL_VALUES_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.WRITEMODEL_STRATEGY_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SinkConfigSoftValidatorTest {
  @Test
  @DisplayName("Incompatible configuration properties must be logged")
  void logIncompatibleProperties() {
    // prepare properties
    Map<String, String> props = new HashMap<>();
    props.put(
        CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
        "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler");
    props.put(DELETE_ON_NULL_VALUES_CONFIG, "true");
    props.put(
        WRITEMODEL_STRATEGY_CONFIG,
        "com.mongodb.kafka.connect.sink.writemodel.strategy.DefaultWriteModelStrategy");
    props.put(
        createOverrideKey("topic1", CHANGE_DATA_CAPTURE_HANDLER_CONFIG),
        "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler");
    props.put(
        createOverrideKey("topic1", DELETE_ON_NULL_VALUES_CONFIG),
        String.valueOf(DELETE_ON_NULL_VALUES_DEFAULT));
    props.put(
        createOverrideKey("topic1", DOCUMENT_ID_STRATEGY_CONFIG), DOCUMENT_ID_STRATEGY_DEFAULT);
    props.put(createOverrideKey("topic2", DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG), "true");
    props.put(createOverrideKey("topic2", WRITEMODEL_STRATEGY_CONFIG), WRITEMODEL_STRATEGY_DEFAULT);
    // configure expectations
    String latterIgnoredMsgFormat =
        "Configuration property %s is incompatible with %s. The latter is ignored.";
    Set<String> expectedMessages = new HashSet<>();
    expectedMessages.add(
        String.format(
            latterIgnoredMsgFormat,
            CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
            DELETE_ON_NULL_VALUES_CONFIG));
    expectedMessages.add(
        String.format(
            latterIgnoredMsgFormat,
            CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
            WRITEMODEL_STRATEGY_CONFIG));
    expectedMessages.add(
        String.format(
            latterIgnoredMsgFormat,
            createOverrideKey("topic1", CHANGE_DATA_CAPTURE_HANDLER_CONFIG),
            createOverrideKey("topic1", DOCUMENT_ID_STRATEGY_CONFIG)));
    expectedMessages.add(
        String.format(
            latterIgnoredMsgFormat,
            createOverrideKey("topic1", CHANGE_DATA_CAPTURE_HANDLER_CONFIG),
            WRITEMODEL_STRATEGY_CONFIG));
    expectedMessages.add(
        String.format(
            latterIgnoredMsgFormat,
            CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
            createOverrideKey("topic2", DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG)));
    expectedMessages.add(
        String.format(
            latterIgnoredMsgFormat,
            CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
            createOverrideKey("topic2", WRITEMODEL_STRATEGY_CONFIG)));
    // run and assert
    Set<String> actualMessages = new HashSet<>();
    SinkConfigSoftValidator.logIncompatibleProperties(props, actualMessages::add);
    assertEquals(expectedMessages, actualMessages);
  }
}
