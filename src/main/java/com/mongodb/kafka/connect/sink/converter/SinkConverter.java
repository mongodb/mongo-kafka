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

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.converter.LazyBsonDocument.Type;

public class SinkConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SinkConverter.class);
  private static final RecordConverter SCHEMA_RECORD_CONVERTER = new SchemaRecordConverter();
  private static final RecordConverter MAP_RECORD_CONVERTER = new MapRecordConverter();
  private static final RecordConverter STRING_RECORD_CONVERTER = new StringRecordConverter();
  private static final RecordConverter BYTE_ARRAY_RECORD_CONVERTER = new ByteArrayRecordConverter();

  public SinkDocument convert(final SinkRecord record) {
    LOGGER.debug(record.toString());

    BsonDocument keyDoc = null;
    if (record.key() != null) {
      keyDoc =
          new LazyBsonDocument(
              record,
              Type.KEY,
              (schema, data) -> getRecordConverter(schema, data).convert(schema, data));
    }

    BsonDocument valueDoc = null;
    if (record.value() != null) {
      valueDoc =
          new LazyBsonDocument(
              record,
              Type.VALUE,
              (Schema schema, Object data) ->
                  getRecordConverter(schema, data).convert(schema, data));
    }

    return new SinkDocument(keyDoc, valueDoc);
  }

  private RecordConverter getRecordConverter(final Schema schema, final Object data) {
    // AVRO or JSON with schema
    if (schema != null && data instanceof Struct) {
      LOGGER.debug("using schemaful converter");
      return SCHEMA_RECORD_CONVERTER;
    }

    // structured JSON without schema
    if (data instanceof Map) {
      LOGGER.debug("using schemaless converter");
      return MAP_RECORD_CONVERTER;
    }

    // raw JSON string
    if (data instanceof String) {
      LOGGER.debug("using raw converter");
      return STRING_RECORD_CONVERTER;
    }

    // raw Bson bytes
    if (data instanceof byte[]) {
      LOGGER.debug("using bson converter");
      return BYTE_ARRAY_RECORD_CONVERTER;
    }

    throw new DataException(
        "No converter present due to unexpected object type: " + data.getClass().getName());
  }
}
