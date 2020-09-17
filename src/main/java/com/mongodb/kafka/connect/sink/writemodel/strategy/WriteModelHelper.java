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

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.BsonElement;

final class WriteModelHelper {

  private static final String CREATE_PREFIX = "%s%s.";
  private static final String ELEMENT_NAME_PREFIX = "%s%s";

  static BsonDocument flattenKeys(final BsonDocument original) {
    BsonDocument businessKey = new BsonDocument();
    original.forEach(
        (key, value) ->
            flattenBsonElement("", new BsonElement(key, value))
                .forEach(b -> businessKey.append(b.getName(), b.getValue())));
    return businessKey;
  }

  static List<BsonElement> flattenBsonElement(final String prefix, final BsonElement element) {
    if (element.getValue().isDocument()) {
      return element.getValue().asDocument().entrySet().stream()
          .flatMap(
              e ->
                  flattenBsonElement(
                      format(CREATE_PREFIX, prefix, element.getName()),
                      new BsonElement(e.getKey(), e.getValue()))
                      .stream())
          .collect(toList());
    }
    return singletonList(
        new BsonElement(
            format(ELEMENT_NAME_PREFIX, prefix, element.getName()), element.getValue()));
  }

  private WriteModelHelper() {}
}
