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

package com.mongodb.kafka.connect.sink.processor;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class NullFieldValueRemover extends PostProcessor {

  public NullFieldValueRemover(final MongoSinkTopicConfig config) {
    super(config);
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    doc.getValueDoc().ifPresent(this::removeNullFieldValues);
  }

  private void removeNullFieldValues(final BsonDocument doc) {
    doc.entrySet()
        .removeIf(
            entry -> {
              BsonValue value = entry.getValue();
              if (value.isDocument()) {
                removeNullFieldValues(value.asDocument());
              }
              if (value.isArray()) {
                value.asArray().stream()
                    .filter(BsonValue::isDocument)
                    .forEach(element -> removeNullFieldValues(element.asDocument()));
              }
              return value.isNull();
            });
  }
}
