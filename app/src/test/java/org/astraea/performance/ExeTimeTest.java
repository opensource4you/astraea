package org.astraea.performance;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExeTimeTest {
  private final ExeTime.Converter converter = new ExeTime.Converter();
  private final ExeTime.Validator validator = new ExeTime.Validator();

  @Test
  void testRecords() {
    Assertions.assertDoesNotThrow(() -> validator.validate("--run.until", "1000records"));
    var exeTime = converter.convert("1000records");
    Assertions.assertEquals(0, exeTime.percentage(0, 10));
    Assertions.assertEquals(100D, exeTime.percentage(1000, 10));
  }

  @Test
  void testDuration() {
    Assertions.assertDoesNotThrow(() -> validator.validate("--run.until", "100ms"));
    var exeTime = converter.convert("100ms");
    Assertions.assertEquals(0, exeTime.percentage(1000, 0));
    Assertions.assertEquals(100D, exeTime.percentage(1000, 100));
  }

  @Test
  void testUnknownArgument() {
    Assertions.assertThrows(
        ParameterException.class, () -> validator.validate("--run.until", "aaa"));
    Assertions.assertThrows(
        ParameterException.class, () -> validator.validate("--run.until", "10record"));
    Assertions.assertThrows(
        ParameterException.class, () -> validator.validate("--run.until", "1000"));
  }
}
