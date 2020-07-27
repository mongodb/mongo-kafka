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

package com.mongodb.kafka.connect.source.producer;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import com.mongodb.kafka.connect.source.MongoSourceConfig;

class BsonSchemaAndValueProducer implements SchemaAndValueProducer {
  private static final Codec<BsonValue> BSON_VALUE_CODEC = new BsonValueCodec();

  @Override
  public SchemaAndValue create(
      final MongoSourceConfig config, final BsonDocument changeStreamDocument) {
    return new SchemaAndValue(Schema.BYTES_SCHEMA, documentToByteBuffer(changeStreamDocument));
  }

  private byte[] documentToByteBuffer(final BsonDocument document) {
    if (document instanceof RawBsonDocument) {
      RawBsonDocument rawBsonDocument = (RawBsonDocument) document;
      ByteBuffer byteBuffer = rawBsonDocument.getByteBuffer().asNIO();
      byte[] byteArray = new byte[byteBuffer.limit()];
      System.arraycopy(
          rawBsonDocument.getByteBuffer().array(), 0, byteArray, 0, byteBuffer.limit());
      return byteArray;
    } else {
      BasicOutputBuffer buffer = new BasicOutputBuffer();
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        BSON_VALUE_CODEC.encode(writer, document, EncoderContext.builder().build());
      }
      return buffer.toByteArray();
    }
  }
}
