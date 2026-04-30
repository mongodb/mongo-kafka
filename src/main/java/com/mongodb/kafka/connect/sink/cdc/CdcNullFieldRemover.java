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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

/**
 * Strips null-valued fields from the document body of a CDC-produced WriteModel.
 *
 * <p>CDC analogue of {@link com.mongodb.kafka.connect.sink.processor.NullFieldValueRemover},
 * applied after the CDC handler has constructed the WriteModel so the operation type (replace /
 * update / delete) has already been decided.
 *
 * <ul>
 *   <li>{@link ReplaceOneModel}: nulls removed from the replacement, recursively.
 *   <li>{@link UpdateOneModel}: nulls removed from inside {@code $set}; {@code $unset} and other
 *       operators are preserved. If {@code $set} becomes empty it is removed; if the whole update
 *       collapses to a no-op, {@code null} is returned.
 *   <li>Other models (delete, etc.): returned unchanged.
 * </ul>
 */
public final class CdcNullFieldRemover {

  private static final String SET = "$set";

  public static WriteModel<BsonDocument> apply(final WriteModel<BsonDocument> model) {
    if (model instanceof ReplaceOneModel) {
      BsonDocument replacement = ((ReplaceOneModel<BsonDocument>) model).getReplacement();
      removeNulls(replacement);
      if (replacement.isEmpty()) {
        return null;
      }
      return model;
    }
    if (model instanceof UpdateOneModel) {
      Bson update = ((UpdateOneModel<BsonDocument>) model).getUpdate();
      if (!(update instanceof BsonDocument)) {
        return model;
      }
      BsonDocument updateDoc = (BsonDocument) update;
      if (updateDoc.containsKey(SET) && updateDoc.get(SET).isDocument()) {
        BsonDocument setDoc = updateDoc.getDocument(SET);
        removeNulls(setDoc);
        if (setDoc.isEmpty()) {
          updateDoc.remove(SET);
        }
      }
      if (updateDoc.isEmpty()) {
        return null;
      }
      return model;
    }
    return model;
  }

  private static void removeNulls(final BsonDocument doc) {
    doc.entrySet()
        .removeIf(
            entry -> {
              BsonValue v = entry.getValue();
              if (v.isDocument()) {
                removeNulls(v.asDocument());
              } else if (v.isArray()) {
                v.asArray().stream()
                    .filter(BsonValue::isDocument)
                    .forEach(e -> removeNulls(e.asDocument()));
              }
              return v.isNull();
            });
  }

  private CdcNullFieldRemover() {}
}
