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

/*
 * Attunity extension by: Abraham Leal
 */
package com.mongodb.kafka.connect.sink.cdc.attunity.rdbms.oracle;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import java.util.HashMap;
import java.util.Map;

public abstract class AttunityCdcHandler extends CdcHandler {
    private static final String OPERATION_TYPE_FIELD_PATH = "operation";
    private static final String OPERATION_TYPE_WRAPPER_PATH = "headers";
    private static final String OPERATION_TYPE_TOPLEVEL_WRAPPER_PATH = "message";

    private final Map<OperationType, CdcOperation> operations = new HashMap<>();

    public AttunityCdcHandler(final MongoSinkTopicConfig config) {
        super(config);
    }

    protected void registerOperations(final Map<OperationType, CdcOperation> operations) {
        this.operations.putAll(operations);
    }

    public CdcOperation getCdcOperation(final BsonDocument doc) {
        try {
            if (!doc.getDocument(OPERATION_TYPE_TOPLEVEL_WRAPPER_PATH).getDocument(OPERATION_TYPE_WRAPPER_PATH).containsKey(OPERATION_TYPE_FIELD_PATH)
                    || !doc.getDocument(OPERATION_TYPE_TOPLEVEL_WRAPPER_PATH).getDocument(OPERATION_TYPE_WRAPPER_PATH).get(OPERATION_TYPE_FIELD_PATH).isString()) {
                throw new DataException("Error: value doc is missing CDC operation type of type string");
            }
            String operation = getAuttnunityOperation(doc.getDocument(OPERATION_TYPE_TOPLEVEL_WRAPPER_PATH).getDocument(OPERATION_TYPE_WRAPPER_PATH).get(OPERATION_TYPE_FIELD_PATH).asString().getValue());
            CdcOperation op = operations.get(OperationType.fromText(operation));
            if (op == null) {
                throw new DataException("Error: no CDC operation found in mapping for operation="
                        + doc.getDocument(OPERATION_TYPE_TOPLEVEL_WRAPPER_PATH).getDocument(OPERATION_TYPE_WRAPPER_PATH).get(OPERATION_TYPE_FIELD_PATH).asString().getValue());
            }
            return op;
        } catch (IllegalArgumentException exc) {
            throw new DataException("Error: parsing CDC operation failed", exc);
        }
    }

    private String getAuttnunityOperation (String fromKey) {
        switch(fromKey){
            case "INSERT":
                return "c";
            case "READ":
                return "r";
            case "UPDATE":
                return "u";
            case "DELETE":
                return "d";
            default:
                throw new IllegalArgumentException("Error: unknown operation type " + fromKey);
        }
    }

}
