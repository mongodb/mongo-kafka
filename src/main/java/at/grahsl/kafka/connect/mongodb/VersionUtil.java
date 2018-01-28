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

package at.grahsl.kafka.connect.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class VersionUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(VersionUtil.class);
  private static String VERSION = "unknown";

  static {
    try {
      Properties props = new Properties();
      props.load(VersionUtil.class.getResourceAsStream("/kafka-connect-mongodb-version.properties"));
      VERSION = props.getProperty("version", VERSION).trim();
    } catch (Exception e) {
      LOGGER.warn("error while loading version:", e);
    }
  }

  public static String getVersion() {
    return VERSION;
  }

}
