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

package com.mongodb.kafka.connect.sink.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

class CdcNullFieldRemoverTest {

  @Test
  @DisplayName("ReplaceOneModel: top-level nulls are removed")
  void replaceFlat() {
    BsonDocument replacement = BsonDocument.parse("{'a': 1, 'b': null, 'c': 'x', 'd': null}");
    ReplaceOneModel<BsonDocument> model =
        new ReplaceOneModel<>(BsonDocument.parse("{'_id': 1}"), replacement);

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertSame(model, result);
    assertEquals(
        BsonDocument.parse("{'a': 1, 'c': 'x'}"),
        ((ReplaceOneModel<BsonDocument>) result).getReplacement());
  }

  @Test
  @DisplayName("ReplaceOneModel: nested nulls are removed recursively")
  void replaceNested() {
    BsonDocument replacement =
        BsonDocument.parse(
            "{"
                + "  'a': null,"
                + "  'sub': {'x': 1, 'y': null, 'inner': {'p': null, 'q': 2}},"
                + "  'arr': [{'k': null, 'v': 1}, {'k': 2, 'v': null}, 'literal']"
                + "}");
    ReplaceOneModel<BsonDocument> model =
        new ReplaceOneModel<>(BsonDocument.parse("{'_id': 1}"), replacement);

    CdcNullFieldRemover.apply(model);

    assertEquals(
        BsonDocument.parse(
            "{"
                + "  'sub': {'x': 1, 'inner': {'q': 2}},"
                + "  'arr': [{'v': 1}, {'k': 2}, 'literal']"
                + "}"),
        model.getReplacement());
  }

  @Test
  @DisplayName("UpdateOneModel: nulls inside $set are removed, $unset preserved")
  void updateSetAndUnset() {
    BsonDocument update =
        BsonDocument.parse("{'$set': {'a': 1, 'b': null, 'c': 'x'}, '$unset': {'d': ''}}");
    UpdateOneModel<BsonDocument> model =
        new UpdateOneModel<>(BsonDocument.parse("{'_id': 1}"), update);

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertSame(model, result);
    assertEquals(
        BsonDocument.parse("{'$set': {'a': 1, 'c': 'x'}, '$unset': {'d': ''}}"),
        ((UpdateOneModel<BsonDocument>) result).getUpdate());
  }

  @Test
  @DisplayName("UpdateOneModel: $set emptied is removed, other operators retained")
  void updateSetEmptied() {
    BsonDocument update = BsonDocument.parse("{'$set': {'a': null}, '$unset': {'b': ''}}");
    UpdateOneModel<BsonDocument> model =
        new UpdateOneModel<>(BsonDocument.parse("{'_id': 1}"), update);

    CdcNullFieldRemover.apply(model);

    assertEquals(BsonDocument.parse("{'$unset': {'b': ''}}"), model.getUpdate());
  }

  @Test
  @DisplayName("UpdateOneModel: collapses to null when only $set with nulls remained")
  void updateCollapsesToNoOp() {
    BsonDocument update = BsonDocument.parse("{'$set': {'a': null, 'b': null}}");
    UpdateOneModel<BsonDocument> model =
        new UpdateOneModel<>(BsonDocument.parse("{'_id': 1}"), update);

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertNull(result);
  }

  @Test
  @DisplayName("UpdateOneModel: nested null inside $set sub-document is removed")
  void updateSetNested() {
    BsonDocument update =
        BsonDocument.parse("{'$set': {'sub': {'x': 1, 'y': null}, 'arr': [{'k': null, 'v': 1}]}}");
    UpdateOneModel<BsonDocument> model =
        new UpdateOneModel<>(BsonDocument.parse("{'_id': 1}"), update);

    CdcNullFieldRemover.apply(model);

    assertEquals(
        BsonDocument.parse("{'$set': {'sub': {'x': 1}, 'arr': [{'v': 1}]}}"), model.getUpdate());
  }

  @Test
  @DisplayName("DeleteOneModel: passed through unchanged")
  void deletePassThrough() {
    DeleteOneModel<BsonDocument> model = new DeleteOneModel<>(BsonDocument.parse("{'_id': 1}"));

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertSame(model, result);
  }

  @Test
  @DisplayName("UpdateOneModel: pipeline-style update (non-BsonDocument) is passed through")
  void updatePipelineStylePassThrough() {
    Bson pipelineUpdate =
        new Bson() {
          @Override
          public <TDocument> BsonDocument toBsonDocument(
              final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument("noop", new BsonInt32(1));
          }
        };
    UpdateOneModel<BsonDocument> model =
        new UpdateOneModel<>(BsonDocument.parse("{'_id': 1}"), pipelineUpdate);

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertSame(model, result);
  }

  @Test
  @DisplayName("UpdateOneModel: update without $set is left unchanged")
  void updateWithoutSet() {
    BsonDocument update = BsonDocument.parse("{'$unset': {'a': ''}, '$inc': {'n': 1}}");
    UpdateOneModel<BsonDocument> model =
        new UpdateOneModel<>(BsonDocument.parse("{'_id': 1}"), update);

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertSame(model, result);
    assertEquals(
        BsonDocument.parse("{'$unset': {'a': ''}, '$inc': {'n': 1}}"),
        ((UpdateOneModel<BsonDocument>) result).getUpdate());
  }

  @Test
  @DisplayName("ReplaceOneModel: empty replacement collapses to null")
  void replaceEmpty() {
    ReplaceOneModel<BsonDocument> model =
        new ReplaceOneModel<>(BsonDocument.parse("{'_id': 1}"), new BsonDocument());

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertNull(result);
  }

  @Test
  @DisplayName("ReplaceOneModel: collapses to null when all fields were null")
  void replaceCollapsesToNoOp() {
    BsonDocument replacement = BsonDocument.parse("{'a': null, 'b': null}");
    ReplaceOneModel<BsonDocument> model =
        new ReplaceOneModel<>(BsonDocument.parse("{'_id': 1}"), replacement);

    WriteModel<BsonDocument> result = CdcNullFieldRemover.apply(model);

    assertNull(result);
  }
}
