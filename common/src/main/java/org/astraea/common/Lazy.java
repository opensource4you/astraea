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
package org.astraea.common;

import java.util.Objects;
import java.util.function.Supplier;

public interface Lazy<T> {

  static <T> Lazy<T> of() {
    return of(null);
  }

  static <T> Lazy<T> of(Supplier<T> supplier) {
    return new Lazy<>() {
      private volatile T obj;

      @Override
      public T get() {
        return get(supplier);
      }

      @Override
      public T get(Supplier<T> supplier) {
        if (obj == null) {
          synchronized (this) {
            if (obj == null) {
              Objects.requireNonNull((supplier));
              obj = Objects.requireNonNull(supplier.get());
            }
          }
        }
        return obj;
      }
    };
  }

  /**
   * the object will get created when this the creation condition is reached. This method will use
   * default supplier to update object. If the default supplier is null, it throws NPE directly.
   *
   * @return object
   */
  T get();

  /**
   * the object will get created when this the creation condition is reached.
   *
   * @param supplier to update internal value. it can't be null
   * @return object
   */
  T get(Supplier<T> supplier);
}
