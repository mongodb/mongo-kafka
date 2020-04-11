package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.SHARD_KEY_CONFIG;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class ReplaceOneShardKeyStrategy implements WriteModelStrategy {

  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

  private MongoSinkTopicConfig config;

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
    BsonDocument vd = document.getValueDoc().orElseThrow(
        () -> new DataException("Error: cannot build the WriteModel since the value document was missing unexpectedly"));

    final String[] shardKeys = config.getString(SHARD_KEY_CONFIG).split(",");
    final BsonDocument shardKeyTargetQuery = new BsonDocument();

    for (String shardKey : shardKeys) {
      shardKeyTargetQuery.put(shardKey, vd.get(shardKey));
    }

    return new ReplaceOneModel<>(shardKeyTargetQuery, vd, REPLACE_OPTIONS);
  }

  public void setConfig(MongoSinkTopicConfig config) {
    this.config = config;
  }
}
