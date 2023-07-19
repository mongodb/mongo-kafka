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

import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;

import com.mongodb.annotations.NotThreadSafe;

import com.mongodb.kafka.connect.source.Configurable;

/** An implementation must have a {@code public} constructor with no formal parameters. */
@NotThreadSafe
public interface TopicMapper extends Configurable {

  /**
   * Returns either a valid {@linkplain SourceRecord#topic() Kafka topic name}, or an {@linkplain
   * String#isEmpty() empty} string, signifying that there is no topic suitable for the {@code
   * changeStreamDocument}, in which case the connector does not produce a corresponding {@link
   * SourceRecord}.
   *
   * @param changeStreamDocument A document produced by a MongoDB change stream with accordance to
   *     the <a
   *     href="https://www.mongodb.com/docs/kafka-connector/current/source-connector/configuration-properties/change-stream/">
   *     change stream configuration</a> and the <a
   *     href="https://www.mongodb.com/docs/kafka-connector/current/source-connector/configuration-properties/startup/">startup
   *     configuration</a>.
   */
  String getTopic(BsonDocument changeStreamDocument);
}
