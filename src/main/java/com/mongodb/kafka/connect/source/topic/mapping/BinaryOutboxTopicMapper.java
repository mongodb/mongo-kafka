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

package com.mongodb.kafka.connect.source.topic.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.source.MongoSourceConfig;

/**
 * topic.mapper = "com.mongodb.kafka.connect.source.topic.mapping.BinaryOutboxTopicMapper"
 * output.format.value = "BINARY_OUTBOX" output.format.key = "BINARY_OUTBOX"
 * binary_outbox.document.topic = "topic" binary_outbox.document.key = "key"
 * binary_outbox.document.value = "value"
 */
public class BinaryOutboxTopicMapper implements TopicMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(BinaryOutboxTopicMapper.class);

  public static final String TOPIC_CONFIG = "binary_outbox.document.topic";
  public static final String TOPIC_CONFIG_DEFAULT = "topic";
  public static final String TOPIC_CONFIG_DOC = "property name of topic in Mongo collection";
  public static final String TOPIC_CONFIG_DISPLAY = "property name of topic in Mongo collection";

  private String topicName;

  @Override
  public void configure(final MongoSourceConfig configuration) {
    topicName = configuration.getString(TOPIC_CONFIG);
    LOGGER.info("topic field is '{}'", topicName);
  }

  @Override
  public String getTopic(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey("fullDocument")) {
      LOGGER.error("document does not contain a topic field named 'fullDocument'");
      return "";
    }

    BsonDocument fullDocument = changeStreamDocument.getDocument("fullDocument");

    if (!fullDocument.containsKey(topicName)) {
      LOGGER.error("document does not contain a topic field named '{}'", topicName);
      return "";
    }

    BsonValue topicNameValue = fullDocument.get(topicName);
    if (!topicNameValue.isString() || topicNameValue.isNull()) {
      LOGGER.error("topic field is not of type String");
      return "";
    }

    LOGGER.info("topic name is: {}", topicNameValue.asString().getValue());
    return topicNameValue.asString().getValue();
  }
}
