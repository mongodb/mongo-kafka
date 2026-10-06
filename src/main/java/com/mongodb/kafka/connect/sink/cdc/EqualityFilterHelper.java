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
package com.mongodb.kafka.connect.sink.cdc;

import static java.lang.String.format;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class EqualityFilterHelper {
  private static final String EQ_OPERATOR = "$eq";

  /**
   * Wraps each value of the document in an equality match. Without it, event-supplied values
   * containing operator-shaped keys would be interpreted as query operators instead of literal
   * value matches.
   */
  public static BsonDocument asEqualityFilter(final BsonDocument document) {
    BsonDocument filter = new BsonDocument();
    document.forEach(
        (field, value) -> {
          // A $-prefixed key is parsed as a top-level query operator, not a field name
          if (field.startsWith("$")) {
            throw new DataException(
                format("Unexpected $-prefixed field `%s`, cannot build a safe filter", field));
          }
          filter.append(field, new BsonDocument(EQ_OPERATOR, value));
        });
    return filter;
  }

  /** Reverses {@link #asEqualityFilter} for a single value, passing through anything else. */
  public static BsonValue unwrapEqualityMatch(final BsonValue value) {
    if (value.isDocument()) {
      BsonDocument doc = value.asDocument();
      if (doc.size() == 1 && doc.containsKey(EQ_OPERATOR)) {
        return doc.get(EQ_OPERATOR);
      }
    }
    return value;
  }

  private EqualityFilterHelper() {}
}
