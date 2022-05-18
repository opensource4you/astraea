package org.astraea;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest {

  @Test
  void testHandleException() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Utils.handleException(
                () -> {
                  throw new ExecutionException(new IllegalArgumentException());
                }));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Utils.handleException(
                () -> {
                  throw new IllegalArgumentException();
                }));

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            Utils.handleException(
                () -> {
                  throw new IOException();
                }));
  }
}
