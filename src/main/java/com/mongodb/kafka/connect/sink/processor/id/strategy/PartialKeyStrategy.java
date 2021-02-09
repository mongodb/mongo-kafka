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

package com.mongodb.kafka.connect.sink.processor.id.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.ALLOWLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.BLOCKLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getOverrideOrDefault;
import static java.lang.String.format;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.AllowListKeyProjector;
import com.mongodb.kafka.connect.sink.processor.BlockListKeyProjector;
import com.mongodb.kafka.connect.sink.processor.field.projection.FieldProjector;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class PartialKeyStrategy implements IdStrategy {

  private FieldProjector fieldProjector;

  public PartialKeyStrategy() {}

  @Override
  public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
    // NOTE: this has to operate on a clone because
    // otherwise it would interfere with further projections
    // happening later in the chain e.g. for key fields
    SinkDocument clone = doc.clone();
    fieldProjector.process(clone, orig);
    // NOTE: If there is no key doc present the strategy
    // simply returns an empty BSON document per default.
    return clone.getKeyDoc().orElseGet(BsonDocument::new);
  }

  public FieldProjector getFieldProjector() {
    return fieldProjector;
  }

  @Override
  public void configure(final MongoSinkTopicConfig config) {
    FieldProjectionType keyProjectionType =
        FieldProjectionType.valueOf(
            getOverrideOrDefault(
                    config,
                    DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG,
                    KEY_PROJECTION_TYPE_CONFIG)
                .toUpperCase());
    String fieldList =
        getOverrideOrDefault(
            config,
            DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG,
            KEY_PROJECTION_LIST_CONFIG);

    switch (keyProjectionType) {
      case BLACKLIST:
      case BLOCKLIST:
        fieldProjector = new BlockListKeyProjector(config, fieldList);
        break;
      case ALLOWLIST:
      case WHITELIST:
        fieldProjector = new AllowListKeyProjector(config, fieldList);
        break;
      default:
        throw new ConnectConfigException(
            DOCUMENT_ID_STRATEGY_CONFIG,
            this.getClass().getName(),
            format(
                "Invalid %s value. It should be set to either %s or %s",
                KEY_PROJECTION_TYPE_CONFIG, BLOCKLIST, ALLOWLIST));
    }
  }
}
