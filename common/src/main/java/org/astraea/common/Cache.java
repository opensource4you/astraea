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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Cache<K, V> {

  public static <K, V> CacheBuilder<K, V> builder() {
    return new CacheBuilder<>();
  }

  public static <K, V> CacheBuilder<K, V> builder(Function<K, V> cacheLoader) {
    return new CacheBuilder<>(cacheLoader);
  }

  public static final class CacheBuilder<A, B> {
    private final Function<A, B> cacheLoader;
    private long expireAfterAccessNanos = -1;
    private int maxCapacity = -1;
    private BiConsumer<A, B> removalListener = (a, b) -> {};

    private CacheBuilder(Function<A, B> cacheLoader) {
      this.cacheLoader = requireNonNull(cacheLoader);
    }

    private CacheBuilder() {
      this.cacheLoader = null;
    }

    public CacheBuilder<A, B> expireAfterAccess(Duration duration) {
      this.expireAfterAccessNanos = duration.toNanos();
      return this;
    }

    public CacheBuilder<A, B> maxCapacity(int maxCapacity) {
      this.maxCapacity = maxCapacity;
      return this;
    }

    public CacheBuilder<A, B> removalListener(BiConsumer<A, B> removalListener) {
      this.removalListener = requireNonNull(removalListener);
      return this;
    }

    public Cache<A, B> build() {
      return new Cache<>(this);
    }
  }

  private final Map<K, Wrapper<V>> map = new HashMap<>();
  private final Function<K, V> cacheLoader;
  private final long expireAfterAccessNanos;
  private final int maxCapacity;
  private final BiConsumer<K, V> removalListener;

  private Cache(CacheBuilder<K, V> builder) {
    this.cacheLoader = builder.cacheLoader;
    this.expireAfterAccessNanos = builder.expireAfterAccessNanos;
    this.maxCapacity = builder.maxCapacity;
    this.removalListener = builder.removalListener;
  }

  public synchronized Optional<V> get(K key) {
    cleanup();
    return Optional.ofNullable(map.get(key)).map(Wrapper::get);
  }

  public synchronized V require(K key, Function<K, V> cacheLoader) {
    cleanup();
    return map.computeIfAbsent(
            key,
            k -> {
              if (maxCapacity > 0 && maxCapacity == map.size()) {
                throw new RuntimeException("cache is full");
              }
              return new Wrapper<>(cacheLoader.apply(k));
            })
        .get();
  }

  public synchronized V require(K key) {
    return require(
        key, Objects.requireNonNull(this.cacheLoader, "There is no default cache loader"));
  }

  public synchronized int size() {
    return map.size();
  }

  // visible for testing
  void cleanup() {
    map.entrySet().stream()
        .filter(e -> e.getValue().expired(System.nanoTime()))
        // collect to list to avoid ConcurrentModificationException
        .collect(Collectors.toList())
        .forEach(
            entry -> {
              try {
                removalListener.accept(entry.getKey(), entry.getValue().get());
              } catch (Throwable err) {
                System.err.printf("Exception thrown by removal listener, %s%n", err.getMessage());
              }
              map.remove(entry.getKey());
            });
  }

  private class Wrapper<T> {
    private final T value;

    private long lastAccess;

    Wrapper(T value) {
      this.value = requireNonNull(value);
      this.lastAccess = System.nanoTime();
    }

    T get() {
      this.lastAccess = System.nanoTime();
      return value;
    }

    boolean expired(long now) {
      return expireAfterAccessNanos > 0 && (now - lastAccess >= expireAfterAccessNanos);
    }
  }
}
