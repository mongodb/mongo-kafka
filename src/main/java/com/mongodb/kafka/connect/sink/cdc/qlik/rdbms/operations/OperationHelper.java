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

package com.mongodb.kafka.connect.sink.cdc.qlik.rdbms.operations;

import static java.lang.String.format;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.qlik.OperationType;

public final class OperationHelper {

  private static final String ID_FIELD = "_id";
  private static final String DATA_BEFORE_FIELD = "beforeData";
  private static final String DATA_FIELD = "data";
  private static final String MESSAGE_FIELD = "message";
  private static final String HEADERS_FIELD = "headers";
  private static final String OPERATION_FIELD = "operation";
  private static final String HEADERS_OPERATION_FIELD = "headers_operation";

  /** The default logical mapping of operations */
  public static final Map<OperationType, CdcOperation> DEFAULT_OPERATIONS =
      Collections.unmodifiableMap(
          new HashMap<OperationType, CdcOperation>() {
            {
              put(OperationType.INSERT, new Replace());
              put(OperationType.REFRESH, new Replace());
              put(OperationType.READ, new Replace());
              put(OperationType.UPDATE, new Update());
              put(OperationType.DELETE, new Delete());
            }
          });

  /**
   * Depending on configuration the operation string can exist in one of the following fields:
   *
   * <pre>
   *  <code>
   *   {message: {operation: 'INSERT', ...}}
   *   {message: {headers: {operation: 'INSERT'}, ...}}
   *   {message: {headers_operation: 'INSERT'}, ...}
   *   {headers: {operation: 'INSERT'}, ...}
   *  </code>
   * </pre>
   *
   * @param valueDocument the value document containing the CDC information
   * @return the operation Type
   */
  public static OperationType getOperationType(final BsonDocument valueDocument) {
    BsonDocument messageDocument = getSubDocumentOrOriginal(MESSAGE_FIELD, valueDocument);
    BsonDocument headersDocument = getSubDocumentOrOriginal(HEADERS_FIELD, messageDocument);

    BsonValue operation = BsonNull.VALUE;
    if (headersDocument.containsKey(OPERATION_FIELD)) {
      operation = headersDocument.get(OPERATION_FIELD);
    } else if (headersDocument.containsKey(HEADERS_OPERATION_FIELD)) {
      operation = headersDocument.get(HEADERS_OPERATION_FIELD);
    }

    if (!operation.isString()) {
      throw new DataException(
          format("Error: Could not determine the CDC operation type. %s", valueDocument.toJson()));
    }

    return OperationType.fromString(operation.asString().getValue());
  }

  static BsonDocument createFilterDocument(final BsonDocument keyDocument) {
    return getFilterFromKeyDocument(keyDocument)
        .orElseGet(() -> new BsonDocument(ID_FIELD, new BsonObjectId()));
  }

  static BsonDocument createUpdateFilterDocument(
      final BsonDocument keyDocument, final BsonDocument valueDocument) {
    BsonDocument filter =
        getFilterFromKeyDocument(keyDocument)
            .orElseGet(
                () -> {
                  BsonDocument messageDocument =
                      getSubDocumentOrOriginal(MESSAGE_FIELD, valueDocument);
                  return getSubDocumentOrOriginal(DATA_BEFORE_FIELD, messageDocument);
                });
    if (filter.isEmpty()) {
      throw new DataException(
          format(
              "Error: Value Document does not contain the expected data, cannot create filter: %s.",
              valueDocument.toJson()));
    }
    return filter;
  }

  static BsonDocument createDeleteFilterDocument(
      final BsonDocument keyDocument, final BsonDocument valueDocument) {
    BsonDocument filter =
        getFilterFromKeyDocument(keyDocument)
            .orElseGet(
                () -> {
                  BsonDocument messageDocument =
                      getSubDocumentOrOriginal(MESSAGE_FIELD, valueDocument);
                  return getSubDocumentNotNullOrOriginal(
                      DATA_BEFORE_FIELD,
                      getSubDocumentNotNullOrOriginal(DATA_FIELD, messageDocument));
                });
    if (filter.isEmpty()) {
      throw new DataException(
          format(
              "Error: Value Document does not contain the expected data, cannot create filter: %s.",
              valueDocument.toJson()));
    }
    return filter;
  }

  static BsonDocument createReplaceDocument(
      final BsonDocument filterDocument, final BsonDocument valueDocument) {
    BsonDocument messageDocument = getSubDocumentOrOriginal(MESSAGE_FIELD, valueDocument);
    BsonDocument afterDocument = getSubDocumentOrOriginal(DATA_FIELD, messageDocument);

    BsonDocument replaceDocument = new BsonDocument();
    if (filterDocument.containsKey(ID_FIELD)) {
      replaceDocument.put(ID_FIELD, filterDocument.get(ID_FIELD));
    }
    for (String f : afterDocument.keySet()) {
      replaceDocument.put(f, afterDocument.get(f));
    }
    return replaceDocument;
  }

  static BsonDocument createUpdateDocument(final BsonDocument valueDocument) {
    BsonDocument messageDocument = getSubDocumentOrOriginal(MESSAGE_FIELD, valueDocument);
    BsonDocument beforeDocument = getSubDocumentOrOriginal(DATA_BEFORE_FIELD, messageDocument);
    BsonDocument afterDocument = getSubDocumentOrOriginal(DATA_FIELD, messageDocument);

    if (afterDocument.isEmpty()) {
      throw new DataException(
          format(
              "Error: Value Document does not contain the expected data, cannot create filter: %s.",
              valueDocument.toJson()));
    }

    BsonDocument updates = new BsonDocument();
    for (String key : afterDocument.keySet()) {
      if (!afterDocument.get(key).equals(beforeDocument.get(key))) {
        updates.put(key, afterDocument.get(key));
      }
    }

    if (updates.isEmpty()) {
      return new BsonDocument();
    }
    return new BsonDocument("$set", updates);
  }

  private static Optional<BsonDocument> getFilterFromKeyDocument(final BsonDocument keyDocument) {
    if (keyDocument.keySet().isEmpty()) {
      return Optional.empty();
    }
    BsonDocument filter = new BsonDocument();
    keyDocument.keySet().forEach(f -> filter.put(f, keyDocument.get(f)));
    return Optional.of(new BsonDocument(ID_FIELD, filter));
  }

  private static BsonDocument getSubDocumentOrOriginal(
      final String fieldName, final BsonDocument original) {
    return getSubDocumentOrOriginal(fieldName, original, false);
  }

  private static BsonDocument getSubDocumentNotNullOrOriginal(
      final String fieldName, final BsonDocument original) {
    return getSubDocumentOrOriginal(fieldName, original, true);
  }

  private static BsonDocument getSubDocumentOrOriginal(
      final String fieldName, final BsonDocument original, final boolean ignoreNull) {
    if (original.containsKey(fieldName)) {
      BsonValue fieldValue = original.get(fieldName);

      if (fieldValue.isNull() && ignoreNull) {
        return original;
      }

      if (!fieldValue.isDocument()) {
        throw new DataException(
            format(
                "Error: Value document contains a '%s' that is not a document: %s",
                fieldName, original));
      }
      return fieldValue.asDocument();
    }
    return original;
  }

  private OperationHelper() {}
}
