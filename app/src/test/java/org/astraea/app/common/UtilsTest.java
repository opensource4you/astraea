package org.astraea.app.common;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest {

  @Test
  void testHandleException() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new ExecutionException(new IllegalArgumentException());
                }));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new IllegalArgumentException();
                }));

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new IOException();
                }));
  }

  @Test
  void testCollectToTreeMap() {
    Assertions.assertInstanceOf(
        SortedMap.class,
        IntStream.range(0, 100).boxed().collect(Utils.toSortedMap(i -> i, i -> i)));
    //noinspection ResultOfMethodCallIgnored
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            Stream.of(Map.entry(1, "hello"), Map.entry(1, "world"))
                .collect(Utils.toSortedMap(Map.Entry::getKey, Map.Entry::getValue)));
  }
}
