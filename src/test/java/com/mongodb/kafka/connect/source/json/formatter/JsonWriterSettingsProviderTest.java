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

package com.mongodb.kafka.connect.source.json.formatter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

@RunWith(JUnitPlatform.class)
public class JsonWriterSettingsProviderTest {

  private static final BsonDocument DEFAULT_TEST_DOCUMENT =
      BsonDocument.parse(
          "{'_id': {'$oid': '5f15aab12435743f9bd126a4'}, "
              + "'myString': 'some foo bla text', "
              + "'myInt': 42, "
              + "'myDouble': {'$numberDouble': '20.21'}, "
              + "'mySubDoc': {'A': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "  'B': {'$date': {'$numberLong': '1577863627000'}}, 'C': {'$numberDecimal': '12345.6789'}}, "
              + "'myArray': [{'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "  {'$date': {'$numberLong': '1577863627000'}}, {'$numberDecimal': '12345.6789'}], "
              + "'myBytes': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "'myDate': {'$date': {'$numberLong': '1577863627000'}}, "
              + "'myDecimal': {'$numberDecimal': '12345.6789'}}");

  private static final BsonDocument EXTENDED_TEST_DOCUMENT =
      BsonDocument.parse(
          "{'_id': {'$oid': '5f15aab12435743f9bd126a4'}, "
              + "'myString': 'some foo bla text', "
              + "'myInt': {'$numberInt': '42'}, "
              + "'myDouble': {'$numberDouble': '20.21'}, "
              + "'mySubDoc': {'A': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "  'B': {'$date': {'$numberLong': '1577863627000'}}, 'C': {'$numberDecimal': '12345.6789'}}, "
              + "'myArray': [{'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "  {'$date': {'$numberLong': '1577863627000'}}, {'$numberDecimal': '12345.6789'}], "
              + "'myBytes': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "'myDate': {'$date': {'$numberLong': '1577863627000'}}, "
              + "'myDecimal': {'$numberDecimal': '12345.6789'}}");

  private static final BsonDocument SIMPLIFIED_TEST_DOCUMENT =
      BsonDocument.parse(
          "{_id: '5f15aab12435743f9bd126a4', "
              + "myString: 'some foo bla text', "
              + "myInt: 42, "
              + "myDouble: 20.21, "
              + "mySubDoc: {A: 'S2Fma2Egcm9ja3Mh', B: '2020-01-01T07:27:07Z', C: '12345.6789'},"
              + "myArray: ['S2Fma2Egcm9ja3Mh', '2020-01-01T07:27:07Z', '12345.6789'],"
              + "myBytes: 'S2Fma2Egcm9ja3Mh', "
              + "myDate: '2020-01-01T07:27:07Z', "
              + "myDecimal: '12345.6789'}");

  @Test
  @DisplayName("test default json settings")
  void testDefaultJson() {
    JsonWriterSettings jsonWriterSettings = new DefaultJson().getJsonWriterSettings();
    assertEquals(DEFAULT_TEST_DOCUMENT.toJson(), EXTENDED_TEST_DOCUMENT.toJson(jsonWriterSettings));
  }

  @Test
  @DisplayName("test extended json settings")
  void testExtendedJson() {
    JsonWriterSettings jsonWriterSettings = new ExtendedJson().getJsonWriterSettings();
    assertEquals(
        EXTENDED_TEST_DOCUMENT.toJson(
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()),
        EXTENDED_TEST_DOCUMENT.toJson(jsonWriterSettings));
  }

  @Test
  @DisplayName("test simplified json settings")
  void testSimplifiedJson() {
    JsonWriterSettings jsonWriterSettings = new SimplifiedJson().getJsonWriterSettings();
    assertEquals(
        SIMPLIFIED_TEST_DOCUMENT.toJson(), EXTENDED_TEST_DOCUMENT.toJson(jsonWriterSettings));
  }
}
