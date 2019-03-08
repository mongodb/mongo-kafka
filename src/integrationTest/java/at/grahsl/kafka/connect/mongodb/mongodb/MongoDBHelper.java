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
package at.grahsl.kafka.connect.mongodb.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MongoDBHelper implements BeforeAllCallback, AfterAllCallback {
    private static final String DEFAULT_URI = "mongodb://localhost:27017/MongoKafkaTest";
    private static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_DATABASE_NAME = "MongoKafkaTest";

    private ConnectionString connectionString;
    private MongoClient mongoClient;

    public MongoDBHelper() {
    }

    private MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(getConnectionString());
        }
        return mongoClient;
    }

    @Override
    public void beforeAll(final ExtensionContext context) {
        getMongoClient();
    }

    @Override
    public void afterAll(final ExtensionContext context) {
        if (mongoClient != null) {
            getDatabase().drop();
            mongoClient.close();
        }
    }

    public MongoDatabase getDatabase() {
        String databaseName = getConnectionString().getDatabase();
        return getMongoClient().getDatabase(databaseName != null ? databaseName : DEFAULT_DATABASE_NAME);
    }

    public ConnectionString getConnectionString() {
        if (connectionString == null) {
            String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                    ? DEFAULT_URI : mongoURIProperty;
            connectionString = new ConnectionString(mongoURIString);
        }
        return connectionString;
    }


}
