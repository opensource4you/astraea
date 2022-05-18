package org.astraea;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class Utils {

  private static Throwable unpack(Throwable exception) {
    Throwable current = exception;
    while (current instanceof ExecutionException) {
      current = current.getCause();
    }
    return current;
  }

  public static <R> R handleException(Getter<R> getter) {
    try {
      return getter.get();
    } catch (Throwable exception) {
      var current = unpack(exception);
      if (current instanceof RuntimeException) throw (RuntimeException) current;
      if (current == null) throw new RuntimeException("unknown error");
      throw new RuntimeException(current);
    }
  }

  public interface Getter<R> {
    R get() throws Exception;
  }

  /**
   * Delete the file or folder
   *
   * @param file path to file or folder
   */
  public static void delete(File file) {
    try {
      if (file.isDirectory()) {
        var fs = file.listFiles();
        if (fs != null) Stream.of(fs).forEach(Utils::delete);
      }
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static File createTempDirectory(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void close(AutoCloseable closeable) {
    try {
      if (closeable != null) closeable.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
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
    var endTime = System.currentTimeMillis() + timeout.toMillis();
    Exception lastError = null;
    while (System.currentTimeMillis() <= endTime)
      try {
        if (done.get()) return;
        TimeUnit.SECONDS.sleep(1);
      } catch (Exception e) {
        lastError = e;
      }
    if (lastError != null) throw new RuntimeException(lastError);
    throw new RuntimeException("Timeout to wait procedure");
  }

  /**
   * Get the field of the object.
   *
   * @param object reflected object.
   * @param fieldName reflected field name.
   * @return Required field.
   */
  public static Object requireField(Object object, String fieldName) {
    try {
      var field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(object);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Separate host and port
   *
   * @param address like 0.0.0.0:9092
   * @return (0.0.0.0,9092)
   */
  public static Map<String, Integer> requireHostPort(List<String> address) {
    var mapAddress = new HashMap<String, Integer>();
    address.forEach(
        str ->
            mapAddress.put(
                Arrays.asList(str.split(":")).get(0),
                Integer.parseInt(Arrays.asList(str.split(":")).get(1))));
    return mapAddress;
  }

  public static int requirePositive(int value) {
    if (value <= 0)
      throw new IllegalArgumentException("the value: " + value + " must be bigger than zero");
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

  public static void sleep(Duration duration) {
    try {
      TimeUnit.MILLISECONDS.sleep(duration.toMillis());
    } catch (InterruptedException ignored) {
    }
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

  private Utils() {}
}
