package com.mongodb.kafka.connect.sink.cdc.debezium.mongodb;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;

public class MongoDbUniqueFieldHandler extends MongoDbHandler {
  private static final Map<OperationType, CdcOperation> DEFAULT_OPERATIONS =
      new HashMap<OperationType, CdcOperation>() {
        {
          put(OperationType.CREATE, new MongoDbInsert());
          put(OperationType.READ, new MongoDbInsert());
          put(OperationType.UPDATE, new MongoDbUpdate(MongoDbUpdate.EventFormat.Oplog, true));
          put(OperationType.DELETE, new MongoDbDelete());
        }
      };

  public MongoDbUniqueFieldHandler(final MongoSinkTopicConfig config) {
    super(config, DEFAULT_OPERATIONS);
  }
}
