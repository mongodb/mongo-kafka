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

package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;

public abstract class Renamer extends PostProcessor {

    //PATH PREFIXES used as a simple means to
    //distinguish whether we operate on key or value
    //structure of a record and match name mappings
    //or regexp patterns accordingly
    public static final String PATH_PREFIX_KEY = "key";
    public static final String PATH_PREFIX_VALUE = "value";

    public static final String SUB_FIELD_DOT_SEPARATOR = ".";

    public Renamer(final MongoDbSinkConnectorConfig config, final String collection) {
        super(config, collection);
    }

    protected abstract String renamed(String path, String name);

    protected abstract boolean isActive();

    protected void doRenaming(final String field, final BsonDocument doc) {
        Map<String, BsonValue> temp = new LinkedHashMap<>();

        Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, BsonValue> entry = iter.next();
            String oldKey = entry.getKey();
            BsonValue value = entry.getValue();
            String newKey = renamed(field, oldKey);

            if (!oldKey.equals(newKey)) {
                //IF NEW KEY ALREADY EXISTS WE THEN DON'T RENAME
                //AS IT WOULD CAUSE OTHER DATA TO BE SILENTLY OVERWRITTEN
                //WHICH IS ALMOST NEVER WHAT YOU WANT
                //MAYBE LOG WARNING HERE?
                doc.computeIfAbsent(newKey, k -> temp.putIfAbsent(k, value));
                iter.remove();
            }

            if (value instanceof BsonDocument) {
                String pathToField = field + SUB_FIELD_DOT_SEPARATOR + newKey;
                doRenaming(pathToField, (BsonDocument) value);
            }
        }

        doc.putAll(temp);
    }

    @Override
    public void process(final SinkDocument doc, final SinkRecord orig) {

        if (isActive()) {
            doc.getKeyDoc().ifPresent(kd -> doRenaming(PATH_PREFIX_KEY, kd));
            doc.getValueDoc().ifPresent(vd -> doRenaming(PATH_PREFIX_VALUE, vd));
        }

    }

}
