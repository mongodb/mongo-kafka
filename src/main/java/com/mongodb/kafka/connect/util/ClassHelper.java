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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package com.mongodb.kafka.connect.util;

import static java.lang.String.format;

import java.util.List;

public final class ClassHelper {

  @SuppressWarnings("unchecked")
  public static <T> T createInstance(
      final String configKey, final String className, final Class<T> clazz) {
    return createInstance(
        configKey,
        className,
        clazz,
        () -> (T) Class.forName(className).getConstructor().newInstance());
  }

  @SuppressWarnings("unchecked")
  public static <T> T createInstance(
      final String configKey,
      final String className,
      final Class<T> clazz,
      final List<Class<?>> constructorArgs,
      final List<Object> initArgs) {
    return createInstance(
        configKey,
        className,
        clazz,
        () ->
            (T)
                Class.forName(className)
                    .getConstructor(constructorArgs.toArray(new Class<?>[0]))
                    .newInstance(initArgs.toArray(new Object[0])));
  }

  public static <T> T createInstance(
      final String configKey,
      final String className,
      final Class<T> clazz,
      final ClassCreator<T> cc) {
    try {
      return cc.init();
    } catch (ClassCastException e) {
      throw new ConnectConfigException(
          configKey,
          className,
          format("Contract violation class doesn't implement: '%s'", clazz.getSimpleName()));
    } catch (ClassNotFoundException e) {
      throw new ConnectConfigException(
          configKey, className, format("Class not found: %s", e.getMessage()));
    } catch (Exception e) {
      if (e.getCause() instanceof ConnectConfigException) {
        throw (ConnectConfigException) e.getCause();
      }
      throw new ConnectConfigException(configKey, className, e.getMessage());
    }
  }

  @FunctionalInterface
  public interface ClassCreator<T> {
    T init() throws Exception;
  }

  private ClassHelper() {}
}
