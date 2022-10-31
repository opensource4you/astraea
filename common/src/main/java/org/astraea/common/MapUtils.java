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

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class MapUtils {

  // ---------------------------------[sorted]---------------------------------//

  public static <T, K, U> Collector<T, ?, SortedMap<K, U>> toSortedMap(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
    return Collectors.toMap(
        keyMapper,
        valueMapper,
        (x, y) -> {
          throw new IllegalStateException("Duplicate key");
        },
        TreeMap::new);
  }

  // ---------------------------------[linked]---------------------------------//

  public static <T, K, U> Collector<T, ?, LinkedHashMap<K, U>> toLinkedHashMap(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
    return Collectors.toMap(
        keyMapper,
        valueMapper,
        (x, y) -> {
          throw new IllegalStateException("Duplicate key");
        },
        LinkedHashMap::new);
  }

  public static <K, V> LinkedHashMap<K, V> of(K k1, V v1) {
    return create(k1, v1);
  }

  public static <K, V> LinkedHashMap<K, V> of(K k1, V v1, K k2, V v2) {
    return create(k1, v1, k2, v2);
  }

  public static <K, V> LinkedHashMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
    return create(k1, v1, k2, v2, k3, v3);
  }

  public static <K, V> LinkedHashMap<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4);
  }

  public static <K, V> LinkedHashMap<K, V> of(
      K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
  }

  public static <K, V> LinkedHashMap<K, V> of(
      K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
  }

  public static <K, V> LinkedHashMap<K, V> of(
      K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
  }

  public static <K, V> LinkedHashMap<K, V> of(
      K k1,
      V v1,
      K k2,
      V v2,
      K k3,
      V v3,
      K k4,
      V v4,
      K k5,
      V v5,
      K k6,
      V v6,
      K k7,
      V v7,
      K k8,
      V v8) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
  }

  public static <K, V> LinkedHashMap<K, V> of(
      K k1,
      V v1,
      K k2,
      V v2,
      K k3,
      V v3,
      K k4,
      V v4,
      K k5,
      V v5,
      K k6,
      V v6,
      K k7,
      V v7,
      K k8,
      V v8,
      K k9,
      V v9) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
  }

  public static <K, V> LinkedHashMap<K, V> of(
      K k1,
      V v1,
      K k2,
      V v2,
      K k3,
      V v3,
      K k4,
      V v4,
      K k5,
      V v5,
      K k6,
      V v6,
      K k7,
      V v7,
      K k8,
      V v8,
      K k9,
      V v9,
      K k10,
      V v10) {
    return create(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
  }

  @SuppressWarnings("unchecked")
  private static <K, V> LinkedHashMap<K, V> create(Object... objs) {
    if (objs.length % 2 != 0) throw new IllegalArgumentException("the length must be even");
    var map = new LinkedHashMap<K, V>(objs.length / 2);
    for (var i = 0; i != objs.length; i += 2)
      map.put((K) Objects.requireNonNull(objs[i]), (V) Objects.requireNonNull(objs[i + 1]));
    return map;
  }

  private MapUtils() {}
}
