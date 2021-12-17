package org.astraea.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DataRateTest {

  static void assertFloatingValueEquals(double expected, double actual) {
    assertTrue(
        Math.abs(expected - actual) < 1e-9,
        () ->
            String.format(
                "floating value might not equal. Expected: \"%f\" but Actual: \"%f\" and difference is \"%f\"",
                expected, actual, Math.abs(expected - actual)));
  }

  @Test
  void fromDurationSafely() {
    assertDoesNotThrow(
        () -> {
          DataRate.fromDurationToBigDecimalSafely(ChronoUnit.FOREVER.getDuration());
        });
    assertDoesNotThrow(
        () -> {
          DataRate.fromDurationToBigIntegerSafely(ChronoUnit.FOREVER.getDuration());
        });
  }

  @Test
  void idealDataRateAndUnit() {
    var sut = DataRate.of(1000, DataUnit.KB, Duration.ofSeconds(1));

    assertFloatingValueEquals(1.0, sut.idealDataRate(ChronoUnit.SECONDS).doubleValue());
    assertSame(DataUnit.MB, sut.idealDataUnit(ChronoUnit.SECONDS));
  }

  @Test
  void dataRate() {
    var sut0 =
        DataRate.of(500, DataUnit.KB, Duration.ofSeconds(1))
            .toBigDecimal(DataUnit.MB, Duration.ofSeconds(1));
    var sut1 =
        DataRate.of(500, DataUnit.KB, ChronoUnit.SECONDS)
            .toBigDecimal(DataUnit.MB, Duration.ofSeconds(1));

    assertFloatingValueEquals(0.5, sut0.doubleValue());
    assertFloatingValueEquals(0.5, sut1.doubleValue());
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        // measurement, dataUnit, passedSecond, expectedIdealDataRate, expectedDataUnit
        "            1, Bit     , 1           ,                 0.125, Byte            ",
        "            1, YB      , 1000000000  ,                     1, PB              ",
        "           16, Bit     , 2           ,                     1, Byte            ",
        "          100, KB      , 1           ,                   100, KB              ",
        "          999, MB      , 1           ,                   999, MB              ",
        "         1000, KB      , 1           ,                     1, MB              ",
        "         1000, KB      , 1           ,                     1, MB              ",
        "         1234, MB      , 1           ,                 1.234, GB              ",
        "         1234, MB      , 1           ,                 1.234, GB              ",
        "         5000, KiB     , 1           ,                  5.12, MB              ",
        "      1000000, Bit     , 8           ,                15.625, KB              ",
        "      1000000, MB      , 1           ,                     1, TB              "
      })
  void testDataRate(
      long measurement,
      DataUnit dataUnit,
      long passedSecond,
      double expectedIdealDataRate,
      DataUnit expectedDataUnit) {
    DataRate sut = DataRate.of(measurement, dataUnit, Duration.ofSeconds(passedSecond));

    assertFloatingValueEquals(
        expectedIdealDataRate, sut.idealDataRate(ChronoUnit.SECONDS).doubleValue());
    assertEquals(expectedDataUnit, sut.idealDataUnit(ChronoUnit.SECONDS));
  }
}
