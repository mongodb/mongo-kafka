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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.OptionalLong;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

public final class ResumeTokenUtils {

  private ResumeTokenUtils() {
    // utils class
  }

  /**
   * The resume token has the following structure:
   *
   * <ol>
   *   <li>It is a document with a single field named "_data" with a string value
   *   <li>The string is hex-encoded
   *   <li>The first byte is the "canonical type" for a BSON timestamp, encoded as an unsigned byte.
   *       It should always equal 130.
   *   <li>The next 8 bytes are the BSON timestamp representing the operation time, encoded as an
   *       unsigned long value with big endian byte order (unlike BSON itself, which is little
   *       endian). The {@link BsonTimestamp} class contains the logic for pulling out the seconds
   *       since the epoch from that value. See <a
   *       href="http://bsonspec.org">http://bsonspec.org</a> for details.
   *   <li>There are more fields encoded in the resume token, but we don't need to decode them as
   *       operation time is always first
   * </ol>
   *
   * @param resumeToken a BSonDocument containing the resume token
   * @return the operation time contained within the resume token
   */
  public static BsonTimestamp getTimestampFromResumeToken(final BsonDocument resumeToken) {
    if (!resumeToken.containsKey("_data")) {
      throw new IllegalArgumentException("Expected _data field in resume token");
    }
    BsonValue data = resumeToken.get("_data");
    byte[] bytes;
    if (data.isString()) {
      // 4.2 servers encode _data as a hex string
      String hexString = data.asString().getValue();
      bytes = parseHex(hexString);
    } else if (data.isBinary()) {
      // 3.6 (and potentially some 4.0?) servers encode _data as binary
      bytes = data.asBinary().getData();
    } else {
      throw new IllegalArgumentException(
          "Expected binary or string for _data field in resume token but found "
              + data.getBsonType());
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

    // cast to an int then remove the sign bit to get the unsigned value
    int canonicalType = ((int) byteBuffer.get()) & 0xff;
    if (canonicalType != 130) {
      throw new IllegalArgumentException(
          "Expected canonical type equal to 130, but found " + canonicalType);
    }

    long timestampAsLong = byteBuffer.asLongBuffer().get();
    return new BsonTimestamp(timestampAsLong);
  }

  public static byte[] parseHex(final String hexString) {
    byte[] bytes = new byte[hexString.length() / 2];
    for (int i = 0, ii = 0; i < bytes.length; i++, ii += 2) {
      int high = Character.digit(hexString.charAt(ii), 16);
      int low = Character.digit(hexString.charAt(ii + 1), 16);
      bytes[i] = (byte) ((high << 4) | low);
    }
    return bytes;
  }

  /**
   * @param response The response from the {@link com.mongodb.event.CommandListener}.
   * @return The offset in seconds. The response operationTime, minus the postBatchResumeToken.
   */
  public static OptionalLong getResponseOffsetSecs(final BsonDocument response) {
    return Optional.of(response)
        .map(v -> v.get("cursor"))
        .map(BsonValue::asDocument)
        .map(v -> v.get("postBatchResumeToken"))
        .map(BsonValue::asDocument)
        .map(
            token ->
                response.get("operationTime").asTimestamp().getTime()
                    - getTimestampFromResumeToken(token).asTimestamp().getTime())
        .map(OptionalLong::of)
        .orElse(OptionalLong.empty());
  }
}
