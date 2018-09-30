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

package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.bson.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class RenamerTest {

    public static BsonDocument keyDoc;
    public static BsonDocument valueDoc;

    public static Map<String, String> fieldnameMappings;
    public static BsonDocument expectedKeyDocFieldnameMapping;
    public static BsonDocument expectedValueDocFieldnameMapping;

    public static Map<String, RenameByRegExp.PatternReplace> regExpSettings;
    public static BsonDocument expectedKeyDocRegExpSettings;
    public static BsonDocument expectedValueDocRegExpSettings;

    @BeforeEach
    public void setupDocumentsToRename() {
        keyDoc = new BsonDocument("fieldA",new BsonString("my field value"));
        keyDoc.put("f2",new BsonBoolean(true));
        keyDoc.put("subDoc",new BsonDocument("fieldX",new BsonInt32(42)));
        keyDoc.put("my_field1",new BsonDocument("my_field2",new BsonString("testing rocks!")));

        valueDoc = new BsonDocument("abc",new BsonString("my field value"));
        valueDoc.put("f2",new BsonBoolean(false));
        valueDoc.put("subDoc",new BsonDocument("123",new BsonDouble(0.0)));
        valueDoc.put("foo.foo.foo",new BsonDocument(".blah..blah.",new BsonInt32(23)));
    }

    @BeforeAll
    public static void setupDocumentsToCompare() {
        expectedKeyDocFieldnameMapping = new BsonDocument("f1",new BsonString("my field value"));
        expectedKeyDocFieldnameMapping.put("fieldB",new BsonBoolean(true));
        expectedKeyDocFieldnameMapping.put("subDoc",new BsonDocument("name_x",new BsonInt32(42)));
        expectedKeyDocFieldnameMapping.put("my_field1",new BsonDocument("my_field2",new BsonString("testing rocks!")));

        expectedValueDocFieldnameMapping = new BsonDocument("xyz",new BsonString("my field value"));
        expectedValueDocFieldnameMapping.put("f_two",new BsonBoolean(false));
        expectedValueDocFieldnameMapping.put("subDoc",new BsonDocument("789",new BsonDouble(0.0)));
        expectedValueDocFieldnameMapping.put("foo.foo.foo",new BsonDocument(".blah..blah.",new BsonInt32(23)));

        expectedKeyDocRegExpSettings = new BsonDocument("FA",new BsonString("my field value"));
        expectedKeyDocRegExpSettings.put("f2",new BsonBoolean(true));
        expectedKeyDocRegExpSettings.put("subDoc",new BsonDocument("FX",new BsonInt32(42)));
        expectedKeyDocRegExpSettings.put("_F1",new BsonDocument("_F2",new BsonString("testing rocks!")));

        expectedValueDocRegExpSettings = new BsonDocument("abc",new BsonString("my field value"));
        expectedValueDocRegExpSettings.put("f2",new BsonBoolean(false));
        expectedValueDocRegExpSettings.put("subDoc",new BsonDocument("123",new BsonDouble(0.0)));
        expectedValueDocRegExpSettings.put("foo_foo_foo",new BsonDocument("_blah__blah_",new BsonInt32(23)));
    }

    @BeforeAll
    public static void setupRenamerSettings() {
        fieldnameMappings = new HashMap<>();
        fieldnameMappings.put(Renamer.PATH_PREFIX_KEY+".fieldA","f1");
        fieldnameMappings.put(Renamer.PATH_PREFIX_KEY+".f2","fieldB");
        fieldnameMappings.put(Renamer.PATH_PREFIX_KEY+".subDoc.fieldX","name_x");
        fieldnameMappings.put(Renamer.PATH_PREFIX_VALUE+".abc","xyz");
        fieldnameMappings.put(Renamer.PATH_PREFIX_VALUE+".f2","f_two");
        fieldnameMappings.put(Renamer.PATH_PREFIX_VALUE+".subDoc.123","789");

        regExpSettings = new HashMap<>();
        regExpSettings.put("^"+Renamer.PATH_PREFIX_KEY+"\\..*my.*$",new RenameByRegExp.PatternReplace("my",""));
        regExpSettings.put("^"+Renamer.PATH_PREFIX_KEY+"\\..*field.*$",new RenameByRegExp.PatternReplace("field","F"));
        regExpSettings.put("^"+Renamer.PATH_PREFIX_VALUE+"\\..*$",new RenameByRegExp.PatternReplace("\\.","_"));
    }



    @Test
    @DisplayName("simple field renamer test with custom field name mappings")
    public void testRenamerUsingFieldnameMapping() {

        SinkDocument sd = new SinkDocument(keyDoc, valueDoc);
        Renamer renamer = new RenameByMapping(null, fieldnameMappings, "");
        renamer.process(sd, null);

        assertAll("key and value doc checks",
                () -> assertEquals(expectedKeyDocFieldnameMapping,sd.getKeyDoc().orElse(new BsonDocument())),
                () -> assertEquals(expectedValueDocFieldnameMapping,sd.getValueDoc().orElse(new BsonDocument()))
        );

    }

    @Test
    @DisplayName("simple field renamer test with custom regexp settings")
    public void testRenamerUsingRegExpSettings() {

        SinkDocument sd = new SinkDocument(keyDoc, valueDoc);
        Renamer renamer = new RenameByRegExp(null, regExpSettings, "");
        renamer.process(sd, null);

        assertAll("key and value doc checks",
                () -> assertEquals(expectedKeyDocRegExpSettings,sd.getKeyDoc().orElse(new BsonDocument())),
                () -> assertEquals(expectedValueDocRegExpSettings,sd.getValueDoc().orElse(new BsonDocument()))
        );

    }

}
