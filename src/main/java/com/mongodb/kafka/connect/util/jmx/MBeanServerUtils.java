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

package com.mongodb.kafka.connect.util.jmx;

import java.lang.management.ManagementFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MBeanServerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MBeanServerUtils.class);

  private MBeanServerUtils() {
    // util class
  }

  public static <T> T registerMBean(final T mBean, final String mBeanName) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      server.registerMBean(mBean, new ObjectName(mBeanName));
      return mBean;
    } catch (InstanceAlreadyExistsException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      // JMX might not be available
      LOGGER.warn("Unable to register MBean " + mBeanName, e);
      return mBean;
    }
  }

  public static void unregisterMBean(final String mBeanName) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      ObjectName objectName = new ObjectName(mBeanName);
      if (server.isRegistered(objectName)) {
        server.unregisterMBean(objectName);
      }
    } catch (Exception e) {
      // JMX might not be available
      LOGGER.warn("Unable to unregister MBean " + mBeanName, e);
    }
  }
}
