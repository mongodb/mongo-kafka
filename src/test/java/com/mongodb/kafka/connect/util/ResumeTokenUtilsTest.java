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

package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.util.ResumeTokenUtils.getResponseOffsetSecs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

public class ResumeTokenUtilsTest {

  private static final String BSON_DOCUMENT_STRING =
      "{\n"
          + "   \"cursor\":{\n"
          + "      \"firstBatch\":[],\n"
          + "      \"postBatchResumeToken\":{\n"
          + "         \"_data\":\"8262EC3E2F000000022B0229296E04\"\n"
          + "      },\n"
          + "      \"id\":3334355540834746110\n"
          + "   },\n"
          + "   \"ok\":1.0,\n"
          + "   \"$clusterTime\":{\n"
          + "      \"clusterTime\":{\n"
          + "         \"$timestamp\":{\n"
          + "            \"t\":1659649683,\n" // postBatchResumeToken + 100
          + "            \"i\":1\n"
          + "         }\n"
          + "      }\n"
          + "   },\n"
          + "   \"operationTime\":{\n"
          + "      \"$timestamp\":{\n"
          + "         \"t\":1659649783,\n" // postBatchResumeToken + 200
          + "         \"i\":1\n"
          + "      }\n"
          + "   }\n"
          + "}";

  @Test
  @DisplayName("Should return the correct offset")
  void testGetResponseOffsetSecs() {
    BsonDocument doc = BsonDocument.parse(BSON_DOCUMENT_STRING);
    assertEquals(200L, getResponseOffsetSecs(BsonDocument.parse(BSON_DOCUMENT_STRING)).getAsLong());

    BsonDocument token = doc.get("cursor").asDocument().get("postBatchResumeToken").asDocument();
    String stringData = token.get("_data").asString().getValue();
    byte[] bytes = ResumeTokenUtils.parseHex(stringData);
    token.put("_data", new BsonBinary(bytes));
    assertEquals(200L, getResponseOffsetSecs(doc).getAsLong());

    token.remove("_data");
    assertThrows(IllegalArgumentException.class, () -> getResponseOffsetSecs(doc));

    token.put("_data", new BsonBoolean(true));
    assertThrows(IllegalArgumentException.class, () -> getResponseOffsetSecs(doc));

    token.put("_data", new BsonString("000000000000000000000000000000"));
    assertThrows(IllegalArgumentException.class, () -> getResponseOffsetSecs(doc));
  }
}
