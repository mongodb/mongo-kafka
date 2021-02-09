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

package com.mongodb.kafka.connect.sink.converter;

import java.util.Optional;

import org.bson.BsonDocument;

public final class SinkDocument implements Cloneable {
  private final BsonDocument keyDoc;
  private final BsonDocument valueDoc;

  public SinkDocument(final BsonDocument keyDoc, final BsonDocument valueDoc) {
    this.keyDoc = keyDoc;
    this.valueDoc = valueDoc;
  }

  public Optional<BsonDocument> getKeyDoc() {
    return Optional.ofNullable(keyDoc);
  }

  public Optional<BsonDocument> getValueDoc() {
    return Optional.ofNullable(valueDoc);
  }

  @Override
  public SinkDocument clone() {
    return new SinkDocument(
        keyDoc != null ? keyDoc.clone() : null, valueDoc != null ? valueDoc.clone() : null);
  }
}
