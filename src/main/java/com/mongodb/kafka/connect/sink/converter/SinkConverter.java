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

public class SinkConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkConverter.class);

    private final RecordConverter schemafulConverter = new AvroJsonSchemafulRecordConverter();
    private final RecordConverter schemalessConverter = new JsonSchemalessRecordConverter();
    private final RecordConverter rawConverter = new JsonRawStringRecordConverter();

    public SinkDocument convert(final SinkRecord record) {
        LOGGER.debug(record.toString());

        BsonDocument keyDoc = null;
        if (record.key() != null) {
            keyDoc = new LazyBsonDocument(() ->
                    getRecordConverter(record.key(), record.keySchema()).convert(record.keySchema(), record.key()));
        }

        BsonDocument valueDoc = null;
        if (record.value() != null) {
            valueDoc = new LazyBsonDocument(() ->
                    getRecordConverter(record.value(), record.valueSchema()).convert(record.valueSchema(), record.value()));
        }

        return new SinkDocument(keyDoc, valueDoc);
    }

    private RecordConverter getRecordConverter(final Object data, final Schema schema) {
        //AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            LOGGER.debug("using schemaful converter");
            return schemafulConverter;
        }

        //structured JSON without schema
        if (data instanceof Map) {
            LOGGER.debug("using schemaless converter");
            return schemalessConverter;
        }

        //raw JSON string
        if (data instanceof String) {
            LOGGER.debug("using raw converter");
            return rawConverter;
        }

        throw new DataException("Error: no converter present due to unexpected object type " + data.getClass().getName());
    }

}
