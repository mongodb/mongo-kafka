package com.mongodb.kafka.connect.sink.writemodel.strategy;


import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class ReplaceOneShardKeyStrategy implements WriteModelStrategy {

  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

  private String[] shardKeys;

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
    BsonDocument vd = document.getValueDoc().orElseThrow(
        () -> new DataException("Error: cannot build the WriteModel since the value document was missing unexpectedly"));

    final BsonDocument shardKeyTargetQuery = new BsonDocument();

    for (String shardKey : shardKeys) {
      final BsonValue shardKeyValue = vd.get(shardKey);

      if (shardKeyValue == null) {
        throw new DataException("Value document does not contain required shard key: " + shardKey);
      }

      shardKeyTargetQuery.put(shardKey, shardKeyValue);
    }

    return new ReplaceOneModel<>(shardKeyTargetQuery, vd, REPLACE_OPTIONS);
  }

  public void setShardKeys(String[] shardKeys) {
    this.shardKeys = shardKeys;
  }
}
