package org.astraea;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.stream.Stream;

public final class Utils {

  public static <R> R handleException(Getter<R> getter) {
    try {
      return getter.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
      ;
    }
  }

  private Utils() {}
}
