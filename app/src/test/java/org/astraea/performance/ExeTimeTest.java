package org.astraea.performance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExeTimeTest {
  private final ExeTime.Converter converter = new ExeTime.Converter();

  @Test
  void testRecords() {
    var exeTime = converter.convert("1000records");
    Assertions.assertEquals(0, exeTime.percentage(0, 10));
    Assertions.assertEquals(100D, exeTime.percentage(1000, 10));
  }

  @Test
  void testDuration() {
    var exeTime = converter.convert("100ms");
    Assertions.assertEquals(0, exeTime.percentage(1000, 0));
    Assertions.assertEquals(100D, exeTime.percentage(1000, 100));
  }

  @Test
  void testUnknownArgument() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert("aaa"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert("10record"));
  }
}
