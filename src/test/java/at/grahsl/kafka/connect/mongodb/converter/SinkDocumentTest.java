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

package at.grahsl.kafka.connect.mongodb.converter;

import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class SinkDocumentTest {

    private static BsonDocument flatStructKey;
    private static BsonDocument flatStructValue;

    private static BsonDocument nestedStructKey;
    private static BsonDocument nestedStructValue;

    @BeforeAll
    public static void initBsonDocs() {

        flatStructKey = new BsonDocument();
        flatStructKey.put("_id", new BsonObjectId(ObjectId.get()));
        flatStructKey.put("myBoolean",new BsonBoolean(true));
        flatStructKey.put("myInt",new BsonInt32(42));
        flatStructKey.put("myBytes",new BsonBinary(new byte[] {65,66,67}));
        BsonArray ba1 = new BsonArray();
        ba1.addAll(Arrays.asList(new BsonInt32(1),new BsonInt32(2),new BsonInt32(3)));
        flatStructKey.put("myArray", ba1);

        flatStructValue = new BsonDocument();
        flatStructValue.put("myLong",new BsonInt64(42L));
        flatStructValue.put("myDouble",new BsonDouble(23.23d));
        flatStructValue.put("myString",new BsonString("BSON"));
        flatStructValue.put("myBytes",new BsonBinary(new byte[] {120,121,122}));
        BsonArray ba2 = new BsonArray();
        ba2.addAll(Arrays.asList(new BsonInt32(9),new BsonInt32(8),new BsonInt32(7)));
        flatStructValue.put("myArray", ba2);

        nestedStructKey = new BsonDocument();
        nestedStructKey.put("_id", new BsonDocument("myString", new BsonString("doc")));
        nestedStructKey.put("mySubDoc", new BsonDocument("mySubSubDoc",
                                            new BsonDocument("myInt",new BsonInt32(23))));

        nestedStructValue = new BsonDocument();
        nestedStructValue.put("mySubDocA", new BsonDocument("myBoolean", new BsonBoolean(false)));
        nestedStructValue.put("mySubDocB", new BsonDocument("mySubSubDocC",
                new BsonDocument("myString",new BsonString("some text..."))));

    }

    @Test
    @DisplayName("test SinkDocument clone with missing key / value")
    public void testCloneNoKeyValue() {

        SinkDocument orig = new SinkDocument(null,null);

        assertAll("orig key/value docs NOT present",
                () -> assertFalse(orig.getKeyDoc().isPresent()),
                () -> assertFalse(orig.getValueDoc().isPresent())
        );

        SinkDocument clone = orig.clone();

        assertAll("clone key/value docs NOT present",
                () -> assertFalse(clone.getKeyDoc().isPresent()),
                () -> assertFalse(clone.getValueDoc().isPresent())
        );

    }

    @Test
    @DisplayName("test SinkDocument clone of flat key / value")
    public void testCloneFlatKeyValue() {

        SinkDocument orig = new SinkDocument(flatStructKey, flatStructValue);

        checkClonedAsserations(orig);

    }

    @Test
    @DisplayName("test SinkDocument clone of nested key / value")
    public void testCloneNestedKeyValue() {

        SinkDocument orig = new SinkDocument(nestedStructKey, nestedStructValue);

        checkClonedAsserations(orig);

    }

    private void checkClonedAsserations(SinkDocument orig) {

        assertAll("orig key/value docs present",
                () -> assertTrue(orig.getKeyDoc().isPresent()),
                () -> assertTrue(orig.getValueDoc().isPresent())
        );

        SinkDocument clone = orig.clone();

        assertAll("clone key/value docs present",
                () -> assertTrue(clone.getKeyDoc().isPresent()),
                () -> assertTrue(clone.getValueDoc().isPresent())
        );

        assertAll("check equality of key/value BSON document structure of clone vs. orig",
                () -> assertTrue(clone.getKeyDoc().get().equals(orig.getKeyDoc().get())),
                () -> assertTrue(clone.getValueDoc().get().equals(orig.getValueDoc().get()))
        );
    }

}
