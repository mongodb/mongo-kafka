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

package com.mongodb.kafka.connect.sink.processor.id.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG;

import java.util.UUID;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonBinary;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;

import com.mongodb.kafka.connect.sink.Configurable;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class UuidStrategy implements IdStrategy, Configurable {
    private MongoSinkTopicConfig.UuidBsonFormat outputFormat;

    @Override
    public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
        UUID uuid = UUID.randomUUID();
        if (outputFormat.equals(MongoSinkTopicConfig.UuidBsonFormat.STRING)) {
            return new BsonString(uuid.toString());
        }

        return new BsonBinary(uuid, UuidRepresentation.STANDARD);
    }

    @Override
    public void configure(final AbstractConfig configuration) {
        outputFormat = MongoSinkTopicConfig.UuidBsonFormat.valueOf(
                configuration.getString(DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG).toUpperCase());
    }
}
