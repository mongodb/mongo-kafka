/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.processor.field.projection;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import com.mongodb.DBCollection;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class WhitelistProjector extends FieldProjector {

    public WhitelistProjector(MongoDbSinkConnectorConfig config,String collection) {
        this(config, config.getValueProjectionList(collection), collection);
    }

    public WhitelistProjector(MongoDbSinkConnectorConfig config,
                              Set<String> fields, String collection) {
        super(config,collection);
        this.fields = fields;
    }

    @Override
    protected void doProjection(String field, BsonDocument doc) {

        //special case short circuit check for '**' pattern
        //this is essentially the same as not using
        //whitelisting at all but instead take the full record
        if(fields.contains(FieldProjector.DOUBLE_WILDCARD)) {
            return;
        }

        Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String, BsonValue> entry = iter.next();

            String key = field.isEmpty() ? entry.getKey()
                    : field + FieldProjector.SUB_FIELD_DOT_SEPARATOR + entry.getKey();
            BsonValue value = entry.getValue();

            if(!fields.contains(key)
                    //NOTE: always keep the _id field
                    && !key.equals(DBCollection.ID_FIELD_NAME)) {

                if(!checkForWildcardMatch(key))
                    iter.remove();

            }

            if(value != null) {
                if(value.isDocument()) {
                    //short circuit check to avoid recursion
                    //if 'key.**' pattern exists
                    String matchDoubleWildCard = key
                            + FieldProjector.SUB_FIELD_DOT_SEPARATOR
                            + FieldProjector.DOUBLE_WILDCARD;
                    if(!fields.contains(matchDoubleWildCard)) {
                        doProjection(key, (BsonDocument)value);
                    }
                }
                if(value.isArray()) {
                    BsonArray values = (BsonArray)value;
                    for(BsonValue v : values.getValues()) {
                        if(v != null && v.isDocument()) {
                            doProjection(key,(BsonDocument)v);
                        }
                    }
                }
            }

        }
    }

    private boolean checkForWildcardMatch(String key) {

        String[] keyParts = key.split("\\"+FieldProjector.SUB_FIELD_DOT_SEPARATOR);
        String[] pattern = new String[keyParts.length];
        Arrays.fill(pattern,FieldProjector.SINGLE_WILDCARD);

        for(int c=(int)Math.pow(2, keyParts.length)-1;c >= 0;c--) {

            int mask = 0x1;
            for(int d = keyParts.length-1;d >= 0;d--) {
                if((c & mask) != 0x0) {
                    pattern[d] = keyParts[d];
                }
                mask <<= 1;
            }

            if(fields.contains(String.join(FieldProjector.SUB_FIELD_DOT_SEPARATOR,pattern)))
                return true;

            Arrays.fill(pattern,FieldProjector.SINGLE_WILDCARD);
        }

        return false;
    }
}
