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

package com.mongodb.kafka.connect.sink.writemodel.strategy;


import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.SHARD_KEY_CONFIG;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.Configurable;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class ReplaceOneShardKeyStrategy implements WriteModelStrategy, Configurable {

    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    private String[] shardKeys;

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException(
                        "Error: cannot build the WriteModel since the value document was missing unexpectedly"));
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

    @Override
    public void configure(final AbstractConfig configuration) {
        String[] shardKeys = configuration.getString(SHARD_KEY_CONFIG).split(",");
        setShardKeys(shardKeys);
    }

    protected void setShardKeys(final String[] shardKeys) {
        this.shardKeys = shardKeys;
    }

    protected String[] getShardKeys() {
        return this.shardKeys;
    }
}
