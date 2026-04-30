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

package com.mongodb.kafka.connect.util;

import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Recursively removes null-valued fields from a {@link BsonDocument} in place.
 *
 * <p>Nested documents and document elements inside arrays are cleaned recursively. Null elements
 * that appear directly inside an array are preserved, since arrays are positional and removing
 * entries would shift indices.
 */
public final class BsonDocumentNullValueRemover {

  public static void removeNullValues(final BsonDocument doc) {
    doc.entrySet()
        .removeIf(
            entry -> {
              BsonValue v = entry.getValue();
              if (v.isDocument()) {
                removeNullValues(v.asDocument());
              } else if (v.isArray()) {
                v.asArray().stream()
                    .filter(BsonValue::isDocument)
                    .forEach(e -> removeNullValues(e.asDocument()));
              }
              return v.isNull();
            });
  }

  private BsonDocumentNullValueRemover() {}
}
