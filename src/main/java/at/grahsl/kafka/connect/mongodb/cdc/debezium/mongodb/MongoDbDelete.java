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

package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.cdc.CdcOperation;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

public class MongoDbDelete implements CdcOperation {

    public static final String JSON_ID_FIELD_PATH = "_id";

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc) {

        BsonValue _id = doc.getKeyDoc()
                .map(kd -> kd.get(JSON_ID_FIELD_PATH))
                .orElseThrow(
                        () -> new DataException("error: extracting _id field from key doc")
                );

        //CURRENTLY THE DEBEZIUM MONGODB CDC SOURCE CONNECTOR
        //HAS A POTENTIAL ISSUE BY NOT SPECIFYING THE TYPE OF
        //THE _id FIELD IN THE KEY STRUCTURE CORRECTLY.
        //AS LONG AS THIS ISN'T FIXED WE CAN ONLY SUPPORT
        //TO HANDLE _id FIELDS OF TYPE STRING WHEN DEALING
        //WITH THE DELETE CHANGE EVENTS
        //SINCE THE _id FIELD ISN'T PART OF THE VALUE STRUCT.

        return new DeleteOneModel<>(
            new BsonDocument(DBCollection.ID_FIELD_NAME,_id)
        );

    }

}
