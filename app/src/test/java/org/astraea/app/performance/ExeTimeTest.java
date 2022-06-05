package org.astraea.app.performance;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExeTimeTest {
  private final ExeTime.Field field = new ExeTime.Field();

  @Test
  void testRecords() {
    Assertions.assertDoesNotThrow(() -> field.validate("--run.until", "1000records"));
    var exeTime = field.convert("1000records");
    Assertions.assertEquals(0, exeTime.percentage(0, 10));
    Assertions.assertEquals(100D, exeTime.percentage(1000, 10));
  }

  @Test
  void testDuration() {
    Assertions.assertDoesNotThrow(() -> field.validate("--run.until", "100ms"));
    var exeTime = field.convert("100ms");
    Assertions.assertEquals(0, exeTime.percentage(1000, 0));
    Assertions.assertEquals(100D, exeTime.percentage(1000, 100));
  }

  @Test
  void testUnknownArgument() {
    Assertions.assertThrows(ParameterException.class, () -> field.validate("--run.until", "aaa"));
    Assertions.assertThrows(
        ParameterException.class, () -> field.validate("--run.until", "10record"));
    Assertions.assertThrows(ParameterException.class, () -> field.validate("--run.until", "1000"));
    Assertions.assertThrows(ParameterException.class, () -> field.validate("--run.until", "10Ms"));
  }
}
