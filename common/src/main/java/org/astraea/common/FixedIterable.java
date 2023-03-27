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

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * this interface offers a "lazy" way to represent a bunch of elements. also, it offers common
 * information/methods to simplify the use case.
 */
public interface FixedIterable<T> extends Iterable<T> {
  FixedIterable EMPTY = of(0, () -> List.of().iterator());

  @SuppressWarnings("unchecked")
  static <T> FixedIterable<T> empty() {
    return (FixedIterable<T>) EMPTY;
  }

  static <T> FixedIterable<T> of(int size, Supplier<Iterator<T>> supplier) {
    return new FixedIterable<>() {
      @Override
      public int size() {
        return size;
      }

      @Override
      public Iterator<T> iterator() {
        return supplier.get();
      }
    };
  }

  default boolean isEmpty() {
    return size() <= 0;
  }

  default boolean nonEmpty() {
    return size() > 0;
  }

  int size();

  default Stream<T> stream() {
    return StreamSupport.stream(spliterator(), false);
  }
}
