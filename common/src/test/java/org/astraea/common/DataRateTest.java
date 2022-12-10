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
package org.astraea.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Assertions;
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
    var sut = DataRate.KB.of(1000).perSecond();

    assertFloatingValueEquals(1.0, sut.idealDataRate(ChronoUnit.SECONDS).doubleValue());
    assertSame(DataUnit.MB, sut.idealDataUnit(ChronoUnit.SECONDS));
  }

  @Test
  void dataRate() {
    var sut0 = DataRate.KB.of(500).perSecond().toBigDecimal(DataUnit.MB, Duration.ofSeconds(1));
    var sut1 = DataRate.KB.of(500).perSecond().toBigDecimal(DataUnit.MB, Duration.ofSeconds(1));

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
    DataRate sut = dataUnit.of(measurement).dataRate(Duration.ofSeconds(passedSecond));

    assertFloatingValueEquals(
        expectedIdealDataRate, sut.idealDataRate(ChronoUnit.SECONDS).doubleValue());
    assertEquals(expectedDataUnit, sut.idealDataUnit(ChronoUnit.SECONDS));
  }

  @Test
  void testToDataSize() {
    Assertions.assertEquals(DataUnit.Byte.of(1024), DataRate.Byte.of(1024).perSecond().dataSize());
    Assertions.assertEquals(DataUnit.KiB.of(1024), DataRate.KiB.of(1024).perSecond().dataSize());
    Assertions.assertEquals(DataUnit.EiB.of(24), DataRate.EiB.of(24).perSecond().dataSize());
  }

  @Test
  void testDoubleByteRate() {
    BiConsumer<Double, Double> assertDoubleEqual =
        (a, b) -> Assertions.assertTrue(
            Math.abs(a - b) < 1e-8,
            "The value " + a + " and " + b + " should have no difference above 1e-8");

    assertDoubleEqual.accept(1024.0, DataRate.Byte.of(1024).perSecond().byteRate());
    assertDoubleEqual.accept(1024.0 * 1024, DataRate.KiB.of(1024).perSecond().byteRate());
  }

  @Test
  void testLongByteRate() {
    Assertions.assertEquals(1024L, DataRate.Byte.of(1024).perSecond().byteRate());
    Assertions.assertEquals(1024L * 1024, DataRate.KiB.of(1024).perSecond().byteRate());
  }

  @Test
  void testDataRateOf() {
    BiConsumer<BigInteger, DataRate.DataRateBuilder> test =
        (bits, rateBuilder) -> {
          var perSecond = rateBuilder.perSecond();
          var perMinute = rateBuilder.perMinute();
          var perHour = rateBuilder.perHour();
          var perDay = rateBuilder.perDay();
          var per2Sec = rateBuilder.over(Duration.ofSeconds(2));
          var perSecond2 = rateBuilder.over(ChronoUnit.SECONDS);
          Assertions.assertEquals(
              bits, perSecond.toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger());
          Assertions.assertEquals(
              bits.divide(BigInteger.valueOf(60)),
              perMinute.toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger());
          Assertions.assertEquals(
              bits.divide(BigInteger.valueOf(60 * 60)),
              perHour.toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger());
          Assertions.assertEquals(
              bits.divide(BigInteger.valueOf(60 * 60 * 24)),
              perDay.toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger());
          Assertions.assertEquals(
              bits.divide(BigInteger.valueOf(2)),
              per2Sec.toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger());
          Assertions.assertEquals(
              bits, perSecond2.toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger());
        };

    test.accept(new BigInteger("1"), DataRate.Bit.of(1));
    test.accept(new BigInteger("1000"), DataRate.Kb.of(1));
    test.accept(new BigInteger("1000000"), DataRate.Mb.of(1));
    test.accept(new BigInteger("1000000000"), DataRate.Gb.of(1));
    test.accept(new BigInteger("1000000000000"), DataRate.Tb.of(1));
    test.accept(new BigInteger("1000000000000000"), DataRate.Pb.of(1));
    test.accept(new BigInteger("1000000000000000000"), DataRate.Eb.of(1));
    test.accept(new BigInteger("1000000000000000000000"), DataRate.Zb.of(1));
    test.accept(new BigInteger("1000000000000000000000000"), DataRate.Yb.of(1));
    test.accept(new BigInteger("1024"), DataRate.Kib.of(1));
    test.accept(new BigInteger("1048576"), DataRate.Mib.of(1));
    test.accept(new BigInteger("1073741824"), DataRate.Gib.of(1));
    test.accept(new BigInteger("1099511627776"), DataRate.Tib.of(1));
    test.accept(new BigInteger("1125899906842624"), DataRate.Pib.of(1));
    test.accept(new BigInteger("1152921504606846976"), DataRate.Eib.of(1));
    test.accept(new BigInteger("1180591620717411303424"), DataRate.Zib.of(1));
    test.accept(new BigInteger("1208925819614629174706176"), DataRate.Yib.of(1));
    test.accept(new BigInteger("8"), DataRate.Byte.of(1));
    test.accept(new BigInteger("8000"), DataRate.KB.of(1));
    test.accept(new BigInteger("8000000"), DataRate.MB.of(1));
    test.accept(new BigInteger("8000000000"), DataRate.GB.of(1));
    test.accept(new BigInteger("8000000000000"), DataRate.TB.of(1));
    test.accept(new BigInteger("8000000000000000"), DataRate.PB.of(1));
    test.accept(new BigInteger("8000000000000000000"), DataRate.EB.of(1));
    test.accept(new BigInteger("8000000000000000000000"), DataRate.ZB.of(1));
    test.accept(new BigInteger("8000000000000000000000000"), DataRate.YB.of(1));
    test.accept(new BigInteger("8192"), DataRate.KiB.of(1));
    test.accept(new BigInteger("8388608"), DataRate.MiB.of(1));
    test.accept(new BigInteger("8589934592"), DataRate.GiB.of(1));
    test.accept(new BigInteger("8796093022208"), DataRate.TiB.of(1));
    test.accept(new BigInteger("9007199254740992"), DataRate.PiB.of(1));
    test.accept(new BigInteger("9223372036854775808"), DataRate.EiB.of(1));
    test.accept(new BigInteger("9444732965739290427392"), DataRate.ZiB.of(1));
    test.accept(new BigInteger("9671406556917033397649408"), DataRate.YiB.of(1));

    DataSize size = DataSize.GB.of(1024);
    test.accept(size.bits(), DataRate.Size.of(size));
  }

  @Test
  void testZero() {
    Assertions.assertEquals(0, DataRate.ZERO.byteRate());
  }
}
