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

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.astraea.common.cost.Configuration;

public final class Utils {

  /**
   * Convert the exception thrown by getter to RuntimeException, except ExecutionException.
   * ExecutionException will be converted to ExecutionRuntimeException , in order to preserve the
   * stacktrace of ExecutionException. This method can eliminate the exception from Java signature.
   *
   * @param getter to execute
   * @param <R> type of returned object
   * @return the object created by getter
   */
  public static <R> R packException(Getter<R> getter) {
    try {
      return getter.get();
    } catch (RuntimeException exception) {
      throw exception;
    } catch (ExecutionException exception) {
      throw new ExecutionRuntimeException(exception);
    } catch (Throwable exception) {
      throw new RuntimeException(exception);
    }
  }

  /**
   * Convert the exception thrown by getter to RuntimeException. This method can eliminate the
   * exception from Java signature.
   *
   * @param runner to execute
   */
  public static void packException(Runner runner) {
    Utils.packException(
        () -> {
          runner.run();
          return null;
        });
  }

  /**
   * Swallow any exception caused by runner.
   *
   * @param runner to execute
   */
  public static void swallowException(Runner runner) {
    try {
      runner.run();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static <T> T construct(Class<T> target, Configuration configuration) {
    try {
      // case 0: create the class by the given configuration
      var constructor = target.getConstructor(Configuration.class);
      return packException(() -> constructor.newInstance(configuration));
    } catch (NoSuchMethodException e) {
      // case 1: create the class by empty constructor
      return packException(() -> target.getConstructor().newInstance());
    }
  }

  public interface Getter<R> {
    R get() throws Exception;
  }

  public interface Runner {
    void run() throws Exception;
  }

  public static String hostname() {
    return Utils.packException(() -> InetAddress.getLocalHost().getHostName());
  }

  /**
   * Wait for procedure. Default is 10 seconds
   *
   * @param done a flag indicating the result.
   */
  public static void waitFor(Supplier<Boolean> done) {
    waitFor(done, Duration.ofSeconds(10));
  }

  /**
   * Wait for procedure.
   *
   * @param done a flag indicating the result.
   */
  public static void waitFor(Supplier<Boolean> done, Duration timeout) {
    waitForNonNull(() -> done.get() ? "good" : null, timeout);
  }

  /**
   * loop the supplier until it returns non-null value. The exception arisen in the waiting get
   * ignored if the supplier offers the non-null value in the end.
   *
   * @param supplier to loop
   * @param timeout to break the loop
   * @param <T> returned type
   * @return value from supplier
   */
  public static <T> T waitForNonNull(Supplier<T> supplier, Duration timeout) {
    var endTime = System.currentTimeMillis() + timeout.toMillis();
    Exception lastError = null;
    while (System.currentTimeMillis() <= endTime)
      try {
        var r = supplier.get();
        if (r != null) return r;
        Utils.sleep(Duration.ofSeconds(1));
      } catch (Exception e) {
        lastError = e;
      }
    if (lastError != null) throw new RuntimeException(lastError);
    throw new RuntimeException("Timeout to wait procedure");
  }

  public static int requirePositive(int value) {
    if (value <= 0)
      throw new IllegalArgumentException("the value: " + value + " must be bigger than zero");
    return value;
  }

  public static short requirePositive(short value) {
    if (value <= 0)
      throw new IllegalArgumentException("the value: " + value + " must be bigger than zero");
    return value;
  }

  public static boolean notNegative(int value) {
    return value >= 0;
  }

  /**
   * check the content of string
   *
   * @param value to check
   * @return input string if the string is not empty. Otherwise, it throws NPE or
   *     IllegalArgumentException
   */
  public static String requireNonEmpty(String value) {
    if (Objects.requireNonNull(value).isEmpty())
      throw new IllegalArgumentException("the value: " + value + " can't be empty");
    return value;
  }

  /**
   * Check if the time is expired.
   *
   * @param lastTime Check time.
   * @param interval Interval.
   * @return Is expired.
   */
  public static boolean isExpired(long lastTime, Duration interval) {
    return (lastTime + interval.toMillis()) < System.currentTimeMillis();
  }

  /**
   * Perform a sleep using the duration. InterruptedException is wrapped to RuntimeException.
   *
   * @param duration to sleep
   */
  public static void sleep(Duration duration) {
    Utils.swallowException(() -> TimeUnit.MILLISECONDS.sleep(duration.toMillis()));
  }

  public static String randomString(int len) {
    StringBuilder string = new StringBuilder(randomString());
    while (string.length() < len) {
      string.append(string).append(randomString());
    }
    return string.substring(0, len);
  }

  /**
   * a random string based on uuid without "-"
   *
   * @return random string
   */
  public static String randomString() {
    return java.util.UUID.randomUUID().toString().replaceAll("-", "");
  }

  public static <T> CompletableFuture<List<T>> sequence(Collection<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .thenApply(f -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  public static <T, U, V> CompletionStage<V> thenCombine(
      CompletionStage<? extends T> from,
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends CompletionStage<V>> fn) {
    return from.thenCompose(l -> other.thenCompose(r -> fn.apply(l, r)));
  }

  public static Object staticMember(Class<?> clz, String attribute) {
    return reflectionAttribute(clz, null, attribute);
  }

  public static Object member(Object object, String attribute) {
    return reflectionAttribute(object.getClass(), object, attribute);
  }

  /**
   * reflection class attribute
   *
   * @param clz class type
   * @param object object. Null means static member
   * @param attribute attribute name
   * @return attribute
   */
  private static Object reflectionAttribute(Class<?> clz, Object object, String attribute) {
    do {
      try {
        var field = clz.getDeclaredField(attribute);
        field.setAccessible(true);
        return field.get(object);
      } catch (NoSuchFieldException e) {
        clz = clz.getSuperclass();
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    } while (clz != null);
    throw new RuntimeException(attribute + " is not existent in " + object.getClass().getName());
  }

  public static <T, K, U> Collector<T, ?, SortedMap<K, U>> toSortedMap(
      Function<? super T, K> keyMapper, Function<? super T, U> valueMapper) {
    return Collectors.toMap(
        keyMapper,
        valueMapper,
        (x, y) -> {
          throw new IllegalStateException("Duplicate key");
        },
        TreeMap::new);
  }

  public static Set<String> constants(Class<?> clz, Predicate<String> variableNameFilter) {
    return Arrays.stream(clz.getFields())
        .filter(field -> variableNameFilter.test(field.getName()))
        .map(field -> packException(() -> field.get(null)))
        .filter(obj -> obj instanceof String)
        .map(obj -> (String) obj)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private Utils() {}
}
