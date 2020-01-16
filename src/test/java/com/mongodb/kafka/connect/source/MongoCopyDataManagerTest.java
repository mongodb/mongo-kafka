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
package com.mongodb.kafka.connect.source;


import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_COLLECTION;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_DATABASE;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.bson.BsonDocument;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
@SuppressWarnings("unchecked")
class MongoCopyDataManagerTest {

    @Mock
    private MongoClient mongoClient;
    @Mock
    private MongoDatabase mongoDatabase;
    @Mock
    private MongoDatabase mongoDatabaseAlt;
    @Mock
    private MongoCollection<BsonDocument> mongoCollection;
    @Mock
    private MongoCollection<BsonDocument> mongoCollectionAlt;
    @Mock
    private AggregateIterable<BsonDocument> aggregateIterable;
    @Mock
    private AggregateIterable<BsonDocument> aggregateIterableAlt;
    @Mock
    private MongoCursor<BsonDocument> cursor;
    @Mock
    private MongoCursor<BsonDocument> cursorAlt;
    @Mock
    private MongoIterable<String> databaseNamesIterable;
    @Mock
    private MongoIterable<String> collectionNamesIterable;
    @Mock
    private MongoIterable<String> collectionNamesIterableAlt;


    @Test
    @DisplayName("test returns the expected collection results")
    void testReturnsTheExpectedCollectionResults() {
        BsonDocument result = BsonDocument.parse("{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'myColl'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");

        when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(TEST_COLLECTION, BsonDocument.class)).thenReturn(mongoCollection);
        when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
        doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
        when(aggregateIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(result);

        List<Optional<BsonDocument>> results;
        try (MongoCopyDataManager copyExistingDataManager = new MongoCopyDataManager(createSourceConfig(), mongoClient)) {
            sleep();
            results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll());
        }

        List<Optional<BsonDocument>> expected = asList(Optional.of(result), Optional.empty());
        assertEquals(expected, results);
    }

    @Test
    @DisplayName("test returns the expected database results")
    void testReturnsTheExpectedDatabaseResults() {
        BsonDocument myDbColl1Result = BsonDocument.parse("{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'coll1'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");

        BsonDocument myDbColl2Result = BsonDocument.parse("{'_id': {'_id': 2, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'coll2'}, "
                + "'documentKey': {'_id': 2}, "
                + "'fullDocument': {'_id': 2, 'a': 'b', 'b': 212}}");


        when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
        when(mongoDatabase.listCollectionNames()).thenReturn(collectionNamesIterable);
        doAnswer(i -> {
            List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
            list.add("coll1");
            list.add("coll2");
            return list;
        }).when(collectionNamesIterable).into(any(ArrayList.class));
        when(mongoDatabase.getCollection("coll1", BsonDocument.class)).thenReturn(mongoCollection);
        when(mongoDatabase.getCollection("coll2", BsonDocument.class)).thenReturn(mongoCollectionAlt);

        when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
        doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
        when(aggregateIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(myDbColl1Result);

        when(mongoCollectionAlt.aggregate(anyList())).thenReturn(aggregateIterableAlt);
        doCallRealMethod().when(aggregateIterableAlt).forEach(any(Consumer.class));
        when(aggregateIterableAlt.iterator()).thenReturn(cursorAlt);
        when(cursorAlt.hasNext()).thenReturn(true, false);
        when(cursorAlt.next()).thenReturn(myDbColl2Result);

        Map<String, String> dbConfig = createConfigMap();
        dbConfig.remove(COLLECTION_CONFIG);

        List<Optional<BsonDocument>> results;
        try (MongoCopyDataManager copyExistingDataManager = new MongoCopyDataManager(new MongoSourceConfig(dbConfig), mongoClient)) {
            sleep();
            results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll(), copyExistingDataManager.poll());
        }
        List<Optional<BsonDocument>> expected = asList(Optional.of(myDbColl1Result), Optional.of(myDbColl2Result), Optional.empty());

        assertTrue(results.containsAll(expected));
        assertEquals(results.get(results.size() - 1), Optional.empty());
    }

    @Test
    @DisplayName("test returns the expected client results")
    void testReturnsTheExpectedClientResults() {
        BsonDocument db1Coll1Result1 = BsonDocument.parse("{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'db1', 'coll': 'coll1'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");
        BsonDocument db1Coll1Result2 = BsonDocument.parse("{'_id': {'_id': 2, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'db1', 'coll': 'coll1'}, "
                + "'documentKey': {'_id': 2}, "
                + "'fullDocument': {'_id': 2, 'a': 'aa', 'b': 111}}");
        BsonDocument db2Coll2Result1 = BsonDocument.parse("{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'db2', 'coll': 'coll2'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'c': 'c', 'd': 999}}");

        when(mongoClient.listDatabaseNames()).thenReturn(databaseNamesIterable);
        doAnswer(i -> {
            List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
            list.add("db1");
            list.add("db2");
            return list;
        }).when(databaseNamesIterable).into(any(ArrayList.class));

        when(mongoClient.getDatabase("db1")).thenReturn(mongoDatabase);
        when(mongoClient.getDatabase("db2")).thenReturn(mongoDatabaseAlt);
        when(mongoDatabase.listCollectionNames()).thenReturn(collectionNamesIterable);
        when(mongoDatabaseAlt.listCollectionNames()).thenReturn(collectionNamesIterableAlt);

        doAnswer(i -> {
            List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
            list.add("coll1");
            return list;
        }).when(collectionNamesIterable).into(any(ArrayList.class));

        doAnswer(i -> {
            List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
            list.add("coll2");
            return list;
        }).when(collectionNamesIterableAlt).into(any(ArrayList.class));

        when(mongoDatabase.getCollection("coll1", BsonDocument.class)).thenReturn(mongoCollection);
        when(mongoDatabaseAlt.getCollection("coll2", BsonDocument.class)).thenReturn(mongoCollectionAlt);

        when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
        doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
        when(aggregateIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(db1Coll1Result1, db1Coll1Result2);

        when(mongoCollectionAlt.aggregate(anyList())).thenReturn(aggregateIterableAlt);
        doCallRealMethod().when(aggregateIterableAlt).forEach(any(Consumer.class));
        when(aggregateIterableAlt.iterator()).thenReturn(cursorAlt);
        when(cursorAlt.hasNext()).thenReturn(true, false);
        when(cursorAlt.next()).thenReturn(db2Coll2Result1);

        List<Optional<BsonDocument>> results;
        try (MongoCopyDataManager copyExistingDataManager = new MongoCopyDataManager(new MongoSourceConfig(new HashMap<>()), mongoClient)) {
            sleep();
            results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll(), copyExistingDataManager.poll(),
                    copyExistingDataManager.poll());
        }
        List<Optional<BsonDocument>> expected = asList(Optional.of(db1Coll1Result1), Optional.of(db1Coll1Result2),
                Optional.of(db2Coll2Result1), Optional.empty());

        assertTrue(results.containsAll(expected));
        assertEquals(results.get(results.size() - 1), Optional.empty());
    }

    private void sleep() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
