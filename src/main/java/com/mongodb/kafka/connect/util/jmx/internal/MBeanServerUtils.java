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

package com.mongodb.kafka.connect.util.jmx.internal;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.lang.Nullable;

import com.mongodb.kafka.connect.util.VisibleForTesting;

public final class MBeanServerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MBeanServerUtils.class);

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  public static final AtomicInteger NEXT_ID = new AtomicInteger();

  private MBeanServerUtils() {
    // util class
  }

  /**
   * Attempts to register the MBean under the provided name, adjusting the name if an instance
   * already exists with that name, or if registration failed.
   *
   * @return the original name, or the adjusted name if the name was adjusted.
   */
  @Nullable
  public static String registerMBean(final DynamicMBean mBean, final String mBeanName) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      String mBeanNameToTry = mBeanName;
      while (true) {
        try {
          server.registerMBean(mBean, new ObjectName(mBeanNameToTry));
          return mBeanNameToTry;
        } catch (InstanceAlreadyExistsException e) {
          LOGGER.warn("MBean name conflict {}", mBeanNameToTry, e);
          mBeanNameToTry = mBeanName + "-v" + NEXT_ID.getAndAdd(1);
        }
      }
    } catch (Exception e) {
      // JMX might not be available
      LOGGER.warn("Unable to register MBean {}", mBeanName, e);
      return mBeanName + "-not-registered";
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
      LOGGER.warn("Unable to unregister MBean {}", mBeanName, e);
    }
  }

  public static String getMBeanDescriptionFor(final String mBeanNameQuery, final String attr) {
    MBeanServer mBeanServer = getPlatformMBeanServer();
    try {
      Set<ObjectName> results = mBeanServer.queryNames(new ObjectName(mBeanNameQuery), null);
      for (ObjectName mBeanName : results) {
        for (MBeanAttributeInfo attributeInfo :
            mBeanServer.getMBeanInfo(mBeanName).getAttributes()) {
          if (attributeInfo.getName().equals(attr)) {
            return attributeInfo.getDescription();
          }
        }
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Map<String, Long>> getMBeanAttributes(final String mBeanNameQuery) {
    return getMBeanAttributes(getPlatformMBeanServer(), mBeanNameQuery);
  }

  public static Map<String, Map<String, Long>> getMBeanAttributes(
      final MBeanServer mBeanServer, final String mBeanNameQuery) {
    try {
      Map<String, Map<String, Long>> mbeansMap = new LinkedHashMap<>();
      Set<ObjectName> results = mBeanServer.queryNames(new ObjectName(mBeanNameQuery), null);
      for (ObjectName mBeanName : results) {
        MBeanInfo info = mBeanServer.getMBeanInfo(mBeanName);
        MBeanAttributeInfo[] attributeInfos = info.getAttributes();
        Map<String, Long> attributes = new LinkedHashMap<>();
        for (MBeanAttributeInfo attributeInfo : attributeInfos) {
          Object value = mBeanServer.getAttribute(mBeanName, attributeInfo.getName());
          if (value instanceof Long) {
            attributes.put(attributeInfo.getName(), (Long) value);
          }
        }
        mbeansMap.put(mBeanName.toString(), attributes);
      }
      return mbeansMap;
    } catch (MBeanException
        | AttributeNotFoundException
        | InstanceNotFoundException
        | ReflectionException
        | MalformedObjectNameException
        | IntrospectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Thread names have the form "task-thread-MongoSinkConnector-0", where the number suffix is the
   * id shown in Kafka's JMX MBean.
   */
  public static String taskIdFromCurrentThread() {
    String s = Thread.currentThread().getName();
    Matcher m = Pattern.compile("(\\d)+").matcher(s);
    return m.find() ? m.group() : "unknown";
  }
}
