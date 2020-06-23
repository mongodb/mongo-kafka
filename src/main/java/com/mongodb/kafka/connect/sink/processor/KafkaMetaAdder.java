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

package com.mongodb.kafka.connect.sink.processor;

import static java.lang.String.format;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonInt64;
import org.bson.BsonString;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class KafkaMetaAdder extends PostProcessor {
  private static final String KAFKA_META_DATA = "topic-partition-offset";

  public KafkaMetaAdder(final MongoSinkTopicConfig config) {
    super(config);
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    doc.getValueDoc()
        .ifPresent(
            vd -> {
              vd.put(
                  KAFKA_META_DATA,
                  new BsonString(
                      format("%s-%s-%s", orig.topic(), orig.kafkaPartition(), orig.kafkaOffset())));
              if (orig.timestampType() != null && orig.timestamp() != null) {
                vd.put(orig.timestampType().name(), new BsonInt64(orig.timestamp()));
              }
            });
  }
}
