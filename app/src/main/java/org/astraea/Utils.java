package org.astraea;

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

  private Utils() {}
}
