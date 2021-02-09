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
package com.mongodb.kafka.connect.sink.processor.field.projection;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;

public abstract class FieldProjector extends PostProcessor {
  private static final String FIELD_LIST_SPLIT_EXPR = "\\s*,\\s*";
  static final String SINGLE_WILDCARD = "*";
  static final String DOUBLE_WILDCARD = "**";
  static final String SUB_FIELD_DOT_SEPARATOR = ".";

  private final Set<String> fields;
  private final FieldProjectionType fieldProjectionType;
  private final SinkDocumentField sinkDocumentField;

  public enum SinkDocumentField {
    KEY,
    VALUE
  }

  public FieldProjector(
      final MongoSinkTopicConfig config,
      final Set<String> fields,
      final FieldProjectionType fieldProjectionType,
      final SinkDocumentField sinkDocumentField) {
    super(config);
    this.fields = fields;
    this.fieldProjectionType = fieldProjectionType;
    this.sinkDocumentField = sinkDocumentField;
  }

  public Set<String> getFields() {
    return fields;
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    switch (fieldProjectionType) {
      case ALLOWLIST:
        getDocumentToProcess(doc).ifPresent(vd -> doProjection("", vd));
        break;
      case BLOCKLIST:
        getDocumentToProcess(doc).ifPresent(vd -> getFields().forEach(f -> doProjection(f, vd)));
        break;
      default:
        // Do nothing
    }
  }

  private Optional<BsonDocument> getDocumentToProcess(final SinkDocument sinkDocument) {
    return sinkDocumentField == SinkDocumentField.KEY
        ? sinkDocument.getKeyDoc()
        : sinkDocument.getValueDoc();
  }

  protected abstract void doProjection(String field, BsonDocument doc);

  public static Set<String> buildProjectionList(
      final FieldProjectionType fieldProjectionType, final String fieldList) {

    Set<String> projectionList;
    switch (fieldProjectionType) {
      case BLOCKLIST:
      case BLACKLIST:
        projectionList = new HashSet<>(toList(fieldList));
        break;
      case ALLOWLIST:
      case WHITELIST:
        // NOTE: for sub document notation all left prefix bound paths are created
        // which allows for easy recursion mechanism to whitelist nested doc fields

        projectionList = new HashSet<>();
        List<String> fields = toList(fieldList);

        for (String f : fields) {
          String entry = f;
          projectionList.add(entry);
          while (entry.contains(".")) {
            entry = entry.substring(0, entry.lastIndexOf("."));
            if (!entry.isEmpty()) {
              projectionList.add(entry);
            }
          }
        }
        break;
      default:
        projectionList = new HashSet<>();
    }
    return projectionList;
  }

  private static List<String> toList(final String value) {
    return Arrays.stream(value.trim().split(FIELD_LIST_SPLIT_EXPR))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
