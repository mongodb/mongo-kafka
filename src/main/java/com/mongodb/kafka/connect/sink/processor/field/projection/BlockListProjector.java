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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

public abstract class BlockListProjector extends FieldProjector {

  public BlockListProjector(
      final MongoSinkTopicConfig config,
      final Set<String> fields,
      final SinkDocumentField sinkDocumentField) {
    super(config, fields, sinkDocumentField);
  }

  @Override
  protected void projectDocument(final BsonDocument doc) {
    getFields().forEach(f -> doProjection(f, doc));
  }

  private void doProjection(final String field, final BsonDocument doc) {
    if (!field.contains(FieldProjector.SUB_FIELD_DOT_SEPARATOR)) {
      processNonNestedMatches(field, doc);
      return;
    }

    int dotIdx = field.indexOf(FieldProjector.SUB_FIELD_DOT_SEPARATOR);
    String firstPart = field.substring(0, dotIdx);
    String otherParts = field.length() >= dotIdx ? field.substring(dotIdx + 1) : "";

    if (firstPart.equals(FieldProjector.SINGLE_WILDCARD)
        || firstPart.equals(FieldProjector.DOUBLE_WILDCARD)) {
      handleWildcard(firstPart, otherParts, doc);
      return;
    }

    BsonValue value = doc.get(firstPart);
    if (value != null) {
      if (value.isDocument()) {
        doProjection(otherParts, value.asDocument());
      }
      if (value.isArray()) {
        BsonArray values = value.asArray();
        for (BsonValue v : values.getValues()) {
          if (v != null && v.isDocument()) {
            doProjection(otherParts, v.asDocument());
          }
        }
      }
    }
  }

  /**
   * Processes any non nested field names and removes any matches from the document.
   *
   * @param field the field name or field name with wildcard
   * @param doc the document to block fields from
   */
  private void processNonNestedMatches(final String field, final BsonDocument doc) {
    if (field.equals(FieldProjector.SINGLE_WILDCARD)
        || field.equals(FieldProjector.DOUBLE_WILDCARD)) {
      handleWildcard(field, "", doc);
    } else if (field.endsWith(FieldProjector.SINGLE_WILDCARD)) {
      String fieldStartsWith = field.substring(0, field.length() - 1);
      doc.entrySet().removeIf(entry -> entry.getKey().startsWith(fieldStartsWith));
    } else {
      doc.remove(field);
    }
  }

  private void handleWildcard(
      final String firstPart, final String otherParts, final BsonDocument doc) {
    Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, BsonValue> entry = iter.next();
      BsonValue value = entry.getValue();

      if (firstPart.equals(FieldProjector.DOUBLE_WILDCARD)) {
        iter.remove();
      } else if (firstPart.equals(FieldProjector.SINGLE_WILDCARD)) {
        if (!value.isDocument()) {
          iter.remove();
        } else if (!otherParts.isEmpty()) {
          doProjection(otherParts, (BsonDocument) value);
        }
      }
    }
  }
}
