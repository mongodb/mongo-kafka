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
package com.mongodb.kafka.connect.sink.cdc.mongodb.operations;

import static java.lang.String.format;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

final class OperationHelper {

  private static final String DOCUMENT_KEY = "documentKey";
  private static final String FULL_DOCUMENT = "fullDocument";
  private static final String UPDATE_DESCRIPTION = "updateDescription";
  private static final String UPDATED_FIELDS = "updatedFields";
  private static final String REMOVED_FIELDS = "removedFields";
  private static final String TRUNCATED_ARRAYS = "truncatedArrays";
  private static final String FIELD = "field";
  private static final String NEW_SIZE = "newSize";
  private static final String SET = "$set";
  private static final String UNSET = "$unset";
  private static final BsonString EMPTY_STRING = new BsonString("");

  static BsonDocument getDocumentKey(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(DOCUMENT_KEY)) {
      throw new DataException(
          format("Missing %s field: %s", DOCUMENT_KEY, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(DOCUMENT_KEY).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expecting a document but found `%s`: %s",
              DOCUMENT_KEY, changeStreamDocument.get(DOCUMENT_KEY), changeStreamDocument.toJson()));
    }

    return changeStreamDocument.getDocument(DOCUMENT_KEY);
  }

  static boolean hasFullDocument(final BsonDocument changeStreamDocument) {
    return changeStreamDocument.containsKey(FULL_DOCUMENT);
  }

  static BsonDocument getFullDocument(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(FULL_DOCUMENT)) {
      throw new DataException(
          format("Missing %s field: %s", FULL_DOCUMENT, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(FULL_DOCUMENT).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expecting a document but found `%s`: %s",
              FULL_DOCUMENT,
              changeStreamDocument.get(FULL_DOCUMENT),
              changeStreamDocument.toJson()));
    }

    return changeStreamDocument.getDocument(FULL_DOCUMENT);
  }

  static BsonDocument getUpdateDocument(final BsonDocument updateDescription) {
    if (!updateDescription.containsKey(UPDATED_FIELDS)) {
      throw new DataException(
          format(
              "Missing %s.%s field: %s",
              UPDATE_DESCRIPTION, UPDATED_FIELDS, updateDescription.toJson()));
    } else if (!updateDescription.get(UPDATED_FIELDS).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected a document but found `%s`: %s",
              UPDATE_DESCRIPTION, updateDescription, updateDescription.toJson()));
    }

    if (!updateDescription.containsKey(REMOVED_FIELDS)) {
      throw new DataException(
          format(
              "Missing %s.%s field: %s",
              UPDATE_DESCRIPTION, REMOVED_FIELDS, updateDescription.toJson()));
    } else if (!updateDescription.get(REMOVED_FIELDS).isArray()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected an array but found `%s`: %s",
              REMOVED_FIELDS, updateDescription.get(REMOVED_FIELDS), updateDescription.toJson()));
    }

    BsonDocument updatedFields = updateDescription.getDocument(UPDATED_FIELDS);
    BsonArray removedFields = updateDescription.getArray(REMOVED_FIELDS);
    BsonDocument unsetDocument = new BsonDocument();
    for (final BsonValue removedField : removedFields) {
      if (!removedField.isString()) {
        throw new DataException(
            format(
                "Unexpected value type in %s, expected an string but found `%s`: %s",
                REMOVED_FIELDS, removedField, updateDescription.toJson()));
      }
      unsetDocument.append(removedField.asString().getValue(), EMPTY_STRING);
    }

    BsonDocument updateDocument = new BsonDocument(SET, updatedFields);
    if (!unsetDocument.isEmpty()) {
      updateDocument.put(UNSET, unsetDocument);
    }

    return updateDocument;
  }

  static BsonDocument getUpdateDescription(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(UPDATE_DESCRIPTION)) {
      throw new DataException(
          format("Missing %s field: %s", UPDATE_DESCRIPTION, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(UPDATE_DESCRIPTION).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected a document found `%s`: %s",
              UPDATE_DESCRIPTION,
              changeStreamDocument.get(UPDATE_DESCRIPTION),
              changeStreamDocument.toJson()));
    }

    return changeStreamDocument.getDocument(UPDATE_DESCRIPTION);
  }

  static boolean hasTruncatedArrays(final BsonDocument updateDocument) {
    return updateDocument.containsKey(TRUNCATED_ARRAYS)
        && updateDocument.isArray(TRUNCATED_ARRAYS)
        && !updateDocument.getArray(TRUNCATED_ARRAYS).isEmpty();
  }

  static BsonDocument getTruncatedArrays(final BsonDocument updateDocument) {
    if (!hasTruncatedArrays(updateDocument)) {
      throw new DataException(
          format("Missing %s array data: %s", TRUNCATED_ARRAYS, updateDocument.toJson()));
    }

    BsonDocument sliceTemplate = BsonDocument.parse("{'$each': []}");
    BsonDocument pushDocument = new BsonDocument();
    BsonArray truncatedArrays = updateDocument.getArray(TRUNCATED_ARRAYS);
    for (BsonValue bsonValue : truncatedArrays) {
      if (!bsonValue.isDocument()) {
        throw new DataException(
            format(
                "Unexpected value type in %s, expected a document but found `%s`: %s",
                TRUNCATED_ARRAYS, bsonValue, updateDocument.toJson()));
      }

      BsonDocument truncatedFieldAndNewSize = bsonValue.asDocument();
      if (!truncatedFieldAndNewSize.containsKey(FIELD)
          || !truncatedFieldAndNewSize.get(FIELD).isString()) {
        throw new DataException(
            format(
                "Unexpected value type in %s, expected a String for %s but found `%s`: %s",
                TRUNCATED_ARRAYS,
                FIELD,
                truncatedFieldAndNewSize.toJson(),
                updateDocument.toJson()));
      }

      if (!truncatedFieldAndNewSize.containsKey(NEW_SIZE)
          || !truncatedFieldAndNewSize.get(NEW_SIZE).isNumber()) {
        throw new DataException(
            format(
                "Unexpected value type in %s, expected a Number for %s but found `%s`: %s",
                TRUNCATED_ARRAYS,
                NEW_SIZE,
                truncatedFieldAndNewSize.toJson(),
                updateDocument.toJson()));
      }

      pushDocument.put(
          truncatedFieldAndNewSize.getString(FIELD).getValue(),
          sliceTemplate.clone().append("$slice", truncatedFieldAndNewSize.getNumber(NEW_SIZE)));
    }
    return new BsonDocument("$push", pushDocument);
  }

  private OperationHelper() {}
}
