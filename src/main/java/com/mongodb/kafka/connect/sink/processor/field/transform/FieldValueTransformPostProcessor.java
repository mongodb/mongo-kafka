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

package com.mongodb.kafka.connect.sink.processor.field.transform;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;

/**
 * A PostProcessor that applies a {@link FieldValueTransformer} to configured fields in the sink
 * document value.
 *
 * <p>This processor is automatically added to the post-processor chain when {@code
 * field.value.transformer} is configured. It traverses the document (including nested documents and
 * arrays) and applies the transformer to any field whose name matches the configured field list.
 */
public class FieldValueTransformPostProcessor extends PostProcessor {
  private final FieldValueTransformer transformer;
  private final Set<String> targetFields;

  public FieldValueTransformPostProcessor(final MongoSinkTopicConfig config) {
    super(config);
    this.transformer = createTransformer(config);
    String fieldsCsv = config.getString(FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);
    this.targetFields = new HashSet<>(Arrays.asList(fieldsCsv.split("\\s*,\\s*")));
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    doc.getValueDoc().ifPresent(this::transformDocument);
  }

  private void transformDocument(final BsonDocument doc) {
    for (String fieldName : doc.keySet()) {
      BsonValue value = doc.get(fieldName);

      if (targetFields.contains(fieldName)) {
        BsonValue transformed = transformer.transform(fieldName, value);
        doc.put(fieldName, transformed);
      } else if (value.isDocument()) {
        transformDocument(value.asDocument());
      } else if (value.isArray()) {
        value.asArray().stream()
            .filter(BsonValue::isDocument)
            .forEach(v -> transformDocument(v.asDocument()));
      }
    }
  }

  private static FieldValueTransformer createTransformer(final MongoSinkTopicConfig config) {
    String className = config.getString(FIELD_VALUE_TRANSFORMER_CONFIG);
    try {
      Class<?> clazz = Class.forName(className);
      if (!FieldValueTransformer.class.isAssignableFrom(clazz)) {
        throw new DataException(className + " does not implement FieldValueTransformer interface");
      }
      FieldValueTransformer instance =
          (FieldValueTransformer) clazz.getDeclaredConstructor().newInstance();
      instance.init(config.originalsStrings());
      return instance;
    } catch (DataException e) {
      throw e;
    } catch (Exception e) {
      throw new DataException("Failed to instantiate FieldValueTransformer: " + className, e);
    }
  }
}
