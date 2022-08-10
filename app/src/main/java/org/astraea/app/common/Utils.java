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

import java.io.File;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.partitioner.Configuration;

public final class Utils {

  static Throwable unpack(Throwable exception) {
    Throwable current = exception;
    while (current instanceof ExecutionException) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Convert the exception thrown by getter to RuntimeException, except ExecutionException.
   * ExecutionException will be converted to AstraeaExecutionRuntimeException , in order to preserve
   * the stacktrace of ExecutionException. This method can eliminate the exception from Java
   * signature.
   *
   * @param getter to execute
   * @param <R> type of returned object
   * @return the object created by getter
   */
  public static <R> R packException(Getter<R> getter) {
    try {
      return getter.get();
    } catch (Throwable exception) {
      if (exception instanceof ExecutionException) {
        throw new AstraeaExecutionRuntimeException((ExecutionException) exception);
      } else if (exception instanceof RuntimeException) {
        throw (RuntimeException) exception;
      } else {
        throw new RuntimeException(exception);
      }
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

  public interface Getter<R> {
    R get() throws Exception;
  }

  public interface Runner {
    void run() throws Exception;
  }

  /**
   * Delete the file or folder
   *
   * @param file path to file or folder
   */
  public static void delete(File file) {
    Utils.packException(
        () -> {
          if (file.isDirectory()) {
            var fs = file.listFiles();
            if (fs != null) Stream.of(fs).forEach(Utils::delete);
          }
          Files.deleteIfExists(file.toPath());
        });
  }

  public static String hostname() {
    return Utils.packException(() -> InetAddress.getLocalHost().getHostName());
  }

  public static File createTempDirectory(String prefix) {
    return Utils.packException(() -> Files.createTempDirectory(prefix).toFile());
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
   * loop the supplier until it returns non-null value
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

  public static int availablePort() {
    return Utils.packException(
        () -> {
          try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            var port = socket.getLocalPort();
            // the port smaller than 1024 may be protected by OS.
            return port > 1024 ? port : port + 1024;
          }
        });
  }

  public static int resolvePort(int port) {
    if (port <= 0) return availablePort();
    return port;
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

  public static <T> CompletableFuture<List<T>> sequence(Collection<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .thenApply(f -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  public static <T extends CostFunction> T constructCostFunction(
      Class<T> costClass, Configuration configuration) {
    try {
      // case 0: create cost function by configuration
      var constructor = costClass.getConstructor(Configuration.class);
      return Utils.packException(() -> constructor.newInstance(configuration));
    } catch (NoSuchMethodException e) {
      // case 1: create cost function by empty constructor
      return Utils.packException(() -> costClass.getConstructor().newInstance());
    }
  }

  private Utils() {}
}
