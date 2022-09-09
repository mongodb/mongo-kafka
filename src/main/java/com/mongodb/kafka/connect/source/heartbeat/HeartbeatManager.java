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

package com.mongodb.kafka.connect.source.heartbeat;

import static com.mongodb.kafka.connect.source.MongoSourceTask.ID_FIELD;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.MongoChangeStreamCursor;

public class HeartbeatManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatManager.class);
  public static final String HEARTBEAT_KEY = "HEARTBEAT";
  private final Time time;
  private final MongoChangeStreamCursor<? extends BsonDocument> cursor;
  private final String heartbeatTopicName;
  private final long heartbeatIntervalMS;
  private final Map<String, Object> partitionMap;

  private long lastHeartbeatMS = 0;
  private String lastResumeToken = "";

  public HeartbeatManager(
      final Time time,
      final MongoChangeStreamCursor<? extends BsonDocument> cursor,
      final long heartbeatIntervalMS,
      final String heartbeatTopicName,
      final Map<String, Object> partitionMap) {
    this.time = time;
    this.cursor = cursor;
    this.heartbeatIntervalMS = heartbeatIntervalMS;
    this.heartbeatTopicName = heartbeatTopicName;
    this.partitionMap = partitionMap;
  }

  public Optional<SourceRecord> heartbeat() {
    if (cursor == null) {
      LOGGER.debug("Returning no heartbeat: null cursor");
      return Optional.empty();
    }
    if (heartbeatIntervalMS <= 0) {
      LOGGER.debug(
          "Returning no heartbeat: heartbeatIntervalMS not positive: {}", heartbeatIntervalMS);
      return Optional.empty();
    }
    long currentMS = time.milliseconds();
    long timeSinceHeartbeatMS = currentMS - lastHeartbeatMS;
    if (timeSinceHeartbeatMS <= heartbeatIntervalMS) {
      LOGGER.debug("Returning no heartbeat: timeSinceHeartbeat has not exceeded heartbeatInterval");
      return Optional.empty();
    }
    lastHeartbeatMS = currentMS;
    BsonDocument resumeTokenBson = cursor.getResumeToken();
    if (resumeTokenBson == null) {
      LOGGER.debug("Returning no heartbeat: cursor resumeToken is null");
      return Optional.empty();
    }
    String resumeToken = resumeTokenBson.toJson();
    if (resumeToken.equals(lastResumeToken)) {
      LOGGER.debug("Returning no heartbeat: same resumeToken");
      return Optional.empty();
    }
    LOGGER.info("Generating heartbeat event. {}", resumeToken);
    Map<String, String> sourceOffset = new HashMap<>();
    sourceOffset.put(ID_FIELD, resumeToken);
    sourceOffset.put(HEARTBEAT_KEY, "true");
    lastResumeToken = resumeToken;
    return Optional.of(
        new SourceRecord(
            partitionMap,
            sourceOffset,
            heartbeatTopicName,
            Schema.STRING_SCHEMA,
            resumeToken,
            Schema.OPTIONAL_BYTES_SCHEMA,
            null));
  }
}
