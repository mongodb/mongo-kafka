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

package com.mongodb.kafka.connect.sink.processor.field.projection;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

public abstract class AllowListProjector extends FieldProjector {

  public AllowListProjector(
      final MongoSinkTopicConfig config,
      final Set<String> fields,
      final SinkDocumentField sinkDocumentField) {
    super(config, fields, sinkDocumentField);
  }

  @Override
  protected void projectDocument(final BsonDocument doc) {
    doProjection("", doc);
  }

  private void doProjection(final String field, final BsonDocument doc) {
    // special case short circuit check for '**' pattern
    // this is essentially the same as not using
    // whitelisting at all but instead take the full record
    if (getFields().contains(FieldProjector.DOUBLE_WILDCARD)) {
      return;
    }

    Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, BsonValue> entry = iter.next();
      String key =
          field.isEmpty()
              ? entry.getKey()
              : field + FieldProjector.SUB_FIELD_DOT_SEPARATOR + entry.getKey();

      BsonValue value = entry.getValue();

      if (!getFields().contains(key) && !checkForWildcardMatch(key)) {
        iter.remove();
      }

      if (value != null) {
        if (value.isDocument()) {
          // short circuit check to avoid recursion
          // if 'key.**' pattern exists
          String matchDoubleWildCard =
              key + FieldProjector.SUB_FIELD_DOT_SEPARATOR + FieldProjector.DOUBLE_WILDCARD;
          if (!getFields().contains(matchDoubleWildCard)) {
            doProjection(key, (BsonDocument) value);
          }
        }
        if (value.isArray()) {
          BsonArray values = value.asArray();
          for (BsonValue v : values.getValues()) {
            if (v != null && v.isDocument()) {
              doProjection(key, (BsonDocument) v);
            }
          }
        }
      }
    }
  }

  private boolean checkForWildcardMatch(final String key) {
    if (checkForStartsWithWildcardMatch(key)) {
      return true;
    }

    String[] keyParts = key.split("\\" + FieldProjector.SUB_FIELD_DOT_SEPARATOR);
    String[] pattern = new String[keyParts.length];
    Arrays.fill(pattern, FieldProjector.SINGLE_WILDCARD);

    for (int c = (int) Math.pow(2, keyParts.length) - 1; c >= 0; c--) {
      int mask = 0x1;
      for (int d = keyParts.length - 1; d >= 0; d--) {
        if ((c & mask) != 0x0) {
          pattern[d] = keyParts[d];
        }
        mask <<= 1;
      }
      if (getFields().contains(String.join(FieldProjector.SUB_FIELD_DOT_SEPARATOR, pattern))) {
        return true;
      }
      Arrays.fill(pattern, FieldProjector.SINGLE_WILDCARD);
    }

    return false;
  }

  /**
   * Processes any non nested field names to see if they should be kept
   *
   * @param key the document key to check against
   * @return true if any of the fields match against the document key
   */
  private boolean checkForStartsWithWildcardMatch(final String key) {
    return getFields().stream()
        .filter(
            f ->
                !f.contains(FieldProjector.SUB_FIELD_DOT_SEPARATOR)
                    && !f.equals(FieldProjector.SINGLE_WILDCARD)
                    && f.endsWith(FieldProjector.SINGLE_WILDCARD))
        .map(f -> f.substring(0, f.length() - 1))
        .anyMatch(key::startsWith);
  }
}
