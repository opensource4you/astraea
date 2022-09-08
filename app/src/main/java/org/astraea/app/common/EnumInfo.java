/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.common;

import java.util.Arrays;
import java.util.Map;

public interface EnumInfo {

  /** This method enforces access to the values method, although it cannot access the class. */
  @SuppressWarnings("unchecked")
  static <T extends Enum<T> & EnumInfo> T ignoreCaseEnum(Class<T> tClass, String alias) {
    return Utils.packException(
        () -> {
          var method = tClass.getDeclaredMethod("values");
          method.setAccessible(true);
          T[] values = (T[]) method.invoke(null);
          return Arrays.stream(values)
              .filter(v -> v.alias().equalsIgnoreCase(alias))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("No such alias: " + alias));
        });
  }

  /** Enum toString method should contain alias. This is the default template. */
  static <T extends Enum<T> & EnumInfo> String alias2String(T t) {
    return Map.of("alias", t.alias()).toString();
  }

  /**
   * You can use {@link #ignoreCaseEnum(Class, String)} to get the Enum by ignore case alias or use
   * {@link #alias2String(Enum)} to override toString method with default template.
   */
  String alias();
}
