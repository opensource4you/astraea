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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DataSizeTest {

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void typicalUsageOfDataUnit() {
    // 500KB
    DataSize.KB.of(500);
    // 500MB + 500MB
    DataSize.MB.of(500).add(500, DataUnit.MB);
    // 500GB - 500GB
    DataSize.GB.of(500).subtract(500, DataUnit.GB);
    // 500 TB / 10
    DataSize.TB.of(500).divide(10);
    // 500 PB * 10
    DataSize.PB.of(500).multiply(10);

    // the data rate of sending 1 EB over 1 second
    DataSize.EB.of(1).dataRate(ChronoUnit.SECONDS);
    // the data rate of sending 1 EB over 1 millisecond
    DataSize.EB.of(1).dataRate(ChronoUnit.MILLIS);
    // the data rate of sending 1 EB over 1 century (100 years)
    DataSize.EB.of(1).dataRate(ChronoUnit.CENTURIES);
    // the data rate of sending 1 EB over 59 seconds
    DataSize.EB.of(1).dataRate(Duration.ofSeconds(59));
    // data rate string of sending 1 EB over 1 second in the most human friendly data unit & second
    // time unit
    DataSize.EB.of(1).dataRate(ChronoUnit.SECONDS).toString();
    // data rate string of sending 1 EB over 1 second in the most human friendly data unit & 1 hour
    // time unit
    DataSize.EB.of(1).dataRate(ChronoUnit.SECONDS).toString(ChronoUnit.HOURS);
    // data rate string of sending 1 EB over 1 second in ZB unit and hour time unit
    DataSize.EB.of(1).dataRate(ChronoUnit.SECONDS).toString(DataUnit.ZB, ChronoUnit.HOURS);

    // someone wondering that if we send 1 YB of data over 1000 years, how much data we sent per
    // second.
    DataSize.YB.of(1).dataRate(ChronoUnit.MILLENNIA).toString(ChronoUnit.SECONDS);

    // faster convert between DataRate and others.
    var randomSize = DataSize.Byte.of(ThreadLocalRandom.current().nextLong());

    // sum all data size
    var sumAll =
        IntStream.range(0, 100).mapToObj(DataSize.Byte::of).reduce(DataSize.ZERO, DataSize::add);
    Assertions.assertEquals(4950, sumAll.bytes());

    // fast way to get bytes, be aware of exception caused by overflow.
    long bytesInLong = randomSize.bytes();
    long bytes10Gib = DataSize.Gib.of(10).bytes();

    // two ways to get second rate
    DataRate two = DataRate.Byte.of(1000).perSecond();

    // data rate to other types
    double dataRateDouble = DataRate.Byte.of(1000).perSecond().byteRate();
    DataSize dataRateSize = DataRate.Byte.of(1000).perSecond().dataSize();

    // fast way to get DataSize & DataRate from primitive type
    DataSize primitive0 = DataSize.Byte.of(1000);
    DataSize primitive1 = DataSize.Byte.of((long) 1000.0);

    // fast way to add/subtract data from primitive type
    DataSize.Byte.of(1000).subtract(500);
    DataSize.Byte.of(1024).add(1024);

    // solve the above problem
    var dataVolume = DataSize.YB.of(1);
    var dataVolumeOver1000Years = dataVolume.dataRate(ChronoUnit.MILLENNIA);
    Consumer<ChronoUnit> tellMeTheAnswerIn =
        (ChronoUnit chronoUnit) ->
            System.out.printf(
                "If Bob sends %s %s data over 1000 years. Then Bob has to send %s %s each %s.%n",
                dataVolume.idealMeasurement(),
                dataVolume.idealDataUnit(),
                dataVolumeOver1000Years.toBigDecimal(
                    dataVolumeOver1000Years.idealDataUnit(chronoUnit), chronoUnit),
                dataVolumeOver1000Years.idealDataUnit(chronoUnit),
                chronoUnit);

    tellMeTheAnswerIn.accept(ChronoUnit.NANOS);
    tellMeTheAnswerIn.accept(ChronoUnit.SECONDS);
    tellMeTheAnswerIn.accept(ChronoUnit.HOURS);
    tellMeTheAnswerIn.accept(ChronoUnit.DAYS);
    tellMeTheAnswerIn.accept(ChronoUnit.MONTHS);
    tellMeTheAnswerIn.accept(ChronoUnit.YEARS);
    tellMeTheAnswerIn.accept(ChronoUnit.DECADES);
    tellMeTheAnswerIn.accept(ChronoUnit.CENTURIES);
    tellMeTheAnswerIn.accept(ChronoUnit.MILLENNIA);

    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.NANOS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.MICROS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.MILLIS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.SECONDS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.MINUTES));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.HOURS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.DAYS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.WEEKS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.MONTHS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.YEARS));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.DECADES));
    System.out.println(dataVolumeOver1000Years.toString(ChronoUnit.CENTURIES));
  }

  @ParameterizedTest
  @CsvSource(
      delimiterString = ",",
      value = {
        // Unit Name, bits for 1 unit
        "  Bit      , 1",
        "  Kb       , 1000",
        "  Mb       , 1000000",
        "  Gb       , 1000000000",
        "  Tb       , 1000000000000",
        "  Pb       , 1000000000000000",
        "  Eb       , 1000000000000000000",
        "  Zb       , 1000000000000000000000",
        "  Yb       , 1000000000000000000000000",
        "  Kib      , 1024",
        "  Mib      , 1048576",
        "  Gib      , 1073741824",
        "  Tib      , 1099511627776",
        "  Pib      , 1125899906842624",
        "  Eib      , 1152921504606846976",
        "  Zib      , 1180591620717411303424",
        "  Yib      , 1208925819614629174706176",
        "  Byte     , 8",
        "  KB       , 8000",
        "  MB       , 8000000",
        "  GB       , 8000000000",
        "  TB       , 8000000000000",
        "  PB       , 8000000000000000",
        "  EB       , 8000000000000000000",
        "  ZB       , 8000000000000000000000",
        "  YB       , 8000000000000000000000000",
        "  KiB      , 8192",
        "  MiB      , 8388608",
        "  GiB      , 8589934592",
        "  TiB      , 8796093022208",
        "  PiB      , 9007199254740992",
        "  EiB      , 9223372036854775808",
        "  ZiB      , 9444732965739290427392",
        "  YiB      , 9671406556917033397649408",
      })
  void of(String unitName, String expectedBits) {
    assertEquals(new BigInteger(expectedBits), DataUnit.valueOf(unitName).of(1).bits());
  }

  @Test
  void dataSizeOf() {
    BiConsumer<BigInteger, BigInteger> test = Assertions::assertEquals;

    test.accept(new BigInteger("1"), DataSize.Bit.of(1).bits());
    test.accept(new BigInteger("1000"), DataSize.Kb.of(1).bits());
    test.accept(new BigInteger("1000000"), DataSize.Mb.of(1).bits());
    test.accept(new BigInteger("1000000000"), DataSize.Gb.of(1).bits());
    test.accept(new BigInteger("1000000000000"), DataSize.Tb.of(1).bits());
    test.accept(new BigInteger("1000000000000000"), DataSize.Pb.of(1).bits());
    test.accept(new BigInteger("1000000000000000000"), DataSize.Eb.of(1).bits());
    test.accept(new BigInteger("1000000000000000000000"), DataSize.Zb.of(1).bits());
    test.accept(new BigInteger("1000000000000000000000000"), DataSize.Yb.of(1).bits());
    test.accept(new BigInteger("1024"), DataSize.Kib.of(1).bits());
    test.accept(new BigInteger("1048576"), DataSize.Mib.of(1).bits());
    test.accept(new BigInteger("1073741824"), DataSize.Gib.of(1).bits());
    test.accept(new BigInteger("1099511627776"), DataSize.Tib.of(1).bits());
    test.accept(new BigInteger("1125899906842624"), DataSize.Pib.of(1).bits());
    test.accept(new BigInteger("1152921504606846976"), DataSize.Eib.of(1).bits());
    test.accept(new BigInteger("1180591620717411303424"), DataSize.Zib.of(1).bits());
    test.accept(new BigInteger("1208925819614629174706176"), DataSize.Yib.of(1).bits());
    test.accept(new BigInteger("8"), DataSize.Byte.of(1).bits());
    test.accept(new BigInteger("8000"), DataSize.KB.of(1).bits());
    test.accept(new BigInteger("8000000"), DataSize.MB.of(1).bits());
    test.accept(new BigInteger("8000000000"), DataSize.GB.of(1).bits());
    test.accept(new BigInteger("8000000000000"), DataSize.TB.of(1).bits());
    test.accept(new BigInteger("8000000000000000"), DataSize.PB.of(1).bits());
    test.accept(new BigInteger("8000000000000000000"), DataSize.EB.of(1).bits());
    test.accept(new BigInteger("8000000000000000000000"), DataSize.ZB.of(1).bits());
    test.accept(new BigInteger("8000000000000000000000000"), DataSize.YB.of(1).bits());
    test.accept(new BigInteger("8192"), DataSize.KiB.of(1).bits());
    test.accept(new BigInteger("8388608"), DataSize.MiB.of(1).bits());
    test.accept(new BigInteger("8589934592"), DataSize.GiB.of(1).bits());
    test.accept(new BigInteger("8796093022208"), DataSize.TiB.of(1).bits());
    test.accept(new BigInteger("9007199254740992"), DataSize.PiB.of(1).bits());
    test.accept(new BigInteger("9223372036854775808"), DataSize.EiB.of(1).bits());
    test.accept(new BigInteger("9444732965739290427392"), DataSize.ZiB.of(1).bits());
    test.accept(new BigInteger("9671406556917033397649408"), DataSize.YiB.of(1).bits());
  }

  @Test
  void parseDataSize() {
    var converter = new DataSize.Field();
    assertEquals(DataUnit.Bit.of(100).toString(), converter.convert("100Bit").toString());
    assertEquals(DataUnit.Kb.of(100).toString(), converter.convert("100 Kb").toString());
    assertEquals(DataUnit.Mb.of(100).toString(), converter.convert("100 Mb").toString());
    assertEquals(DataUnit.Gb.of(100).toString(), converter.convert("100 Gb").toString());
    assertEquals(DataUnit.Tb.of(100).toString(), converter.convert("100 Tb").toString());
    assertEquals(DataUnit.Pb.of(100).toString(), converter.convert("100 Pb").toString());
    assertEquals(DataUnit.Eb.of(100).toString(), converter.convert("100 Eb").toString());
    assertEquals(DataUnit.Zb.of(100).toString(), converter.convert("100 Zb").toString());
    assertEquals(DataUnit.Yb.of(100).toString(), converter.convert("100 Yb").toString());

    assertEquals(DataUnit.Kib.of(100).toString(), converter.convert("100 Kib").toString());
    assertEquals(DataUnit.Mib.of(100).toString(), converter.convert("100 Mib").toString());
    assertEquals(DataUnit.Gib.of(100).toString(), converter.convert("100 Gib").toString());
    assertEquals(DataUnit.Tib.of(100).toString(), converter.convert("100 Tib").toString());
    assertEquals(DataUnit.Pib.of(100).toString(), converter.convert("100 Pib").toString());
    assertEquals(DataUnit.Eib.of(100).toString(), converter.convert("100 Eib").toString());
    assertEquals(DataUnit.Zib.of(100).toString(), converter.convert("100 Zib").toString());
    assertEquals(DataUnit.Yib.of(100).toString(), converter.convert("100 Yib").toString());

    assertEquals(DataUnit.Byte.of(100).toString(), converter.convert("100Byte").toString());
    assertEquals(DataUnit.KB.of(100).toString(), converter.convert("100 KB").toString());
    assertEquals(DataUnit.MB.of(100).toString(), converter.convert("100 MB").toString());
    assertEquals(DataUnit.GB.of(100).toString(), converter.convert("100 GB").toString());
    assertEquals(DataUnit.TB.of(100).toString(), converter.convert("100 TB").toString());
    assertEquals(DataUnit.PB.of(100).toString(), converter.convert("100 PB").toString());
    assertEquals(DataUnit.EB.of(100).toString(), converter.convert("100 EB").toString());
    assertEquals(DataUnit.ZB.of(100).toString(), converter.convert("100 ZB").toString());
    assertEquals(DataUnit.YB.of(100).toString(), converter.convert("100 YB").toString());

    assertEquals(DataUnit.KiB.of(100).toString(), converter.convert("100 KiB").toString());
    assertEquals(DataUnit.MiB.of(100).toString(), converter.convert("100 MiB").toString());
    assertEquals(DataUnit.GiB.of(100).toString(), converter.convert("100 GiB").toString());
    assertEquals(DataUnit.TiB.of(100).toString(), converter.convert("100 TiB").toString());
    assertEquals(DataUnit.PiB.of(100).toString(), converter.convert("100 PiB").toString());
    assertEquals(DataUnit.EiB.of(100).toString(), converter.convert("100 EiB").toString());
    assertEquals(DataUnit.ZiB.of(100).toString(), converter.convert("100 ZiB").toString());
    assertEquals(DataUnit.YiB.of(100).toString(), converter.convert("100 YiB").toString());

    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB xxx"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB per second"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("xxx 5000 MB"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("5000 MB 400GB"));
    assertThrows(IllegalArgumentException.class, () -> converter.convert("6.00 MB"));
  }

  @Test
  void measurement() {
    var value = DataUnit.PB.of(1);

    var assertEquals =
        (BiFunction<BigDecimal, BigDecimal, Void>)
            (a, b) -> {
              double v0 = a.doubleValue();
              double v1 = b.doubleValue();
              assertTrue(Math.abs(v0 - v1) <= 0.00000000001);
              return null;
            };

    assertEquals.apply(new BigDecimal("1000000000"), value.measurement(DataUnit.MB));
    assertEquals.apply(new BigDecimal("1000000"), value.measurement(DataUnit.GB));
    assertEquals.apply(new BigDecimal("1000"), value.measurement(DataUnit.TB));
    assertEquals.apply(new BigDecimal("1"), value.measurement(DataUnit.PB));
    assertEquals.apply(new BigDecimal("0.001"), value.measurement(DataUnit.EB));
    assertEquals.apply(new BigDecimal("0.000001"), value.measurement(DataUnit.ZB));
    assertEquals.apply(new BigDecimal("0.000000001"), value.measurement(DataUnit.YB));
  }

  @Test
  void add() {
    var lhs = DataUnit.KB.of(1024);
    var rhs = DataUnit.GB.of(1);
    assertEquals(
        1024 * 1000 + 1000 * 1000 * 1000L, lhs.add(rhs).measurement(DataUnit.Byte).longValue());
  }

  @Test
  void subtract() {
    var lhs = DataUnit.GB.of(1);
    var rhs = DataUnit.KB.of(1024);
    assertEquals(
        1000 * 1000 * 1000 - 1024 * 1000L,
        lhs.subtract(rhs).measurement(DataUnit.Byte).longValue());
  }

  @Test
  void multiply() {
    var lhs = DataUnit.GB.of(1);
    var rhs = 100;
    assertEquals(
        100 * 1000 * 1000 * 1000L, lhs.multiply(rhs).measurement(DataUnit.Byte).longValue());
  }

  @Test
  void divide() {
    var lhs = DataUnit.GB.of(1);
    var rhs = 100;
    assertEquals(1000 * 1000 * 1000 / 100L, lhs.divide(rhs).measurement(DataUnit.Byte).longValue());
  }

  @Test
  void idealUnit() {
    var value =
        DataUnit.Byte.of(1)
            .multiply(1000) // 1 KB
            .multiply(1000) // 1 MB
            .multiply(1000) // 1 GB
            .multiply(1000) // 1 TB
            .multiply(1000) // 1 PB
            .multiply(1000) // 1 EB
            .multiply(1000) // 1 ZB
            .multiply(1000) // 1 YB
            .subtract(1, DataUnit.Bit); // 1 YB - 1 bit

    assertSame(DataUnit.ZB, value.idealDataUnit());
  }

  @Test
  void idealMeasurement() {
    var value = DataUnit.Byte.of(1).multiply(1000).multiply(1000).multiply(1000);

    assertSame(DataUnit.GB, value.idealDataUnit());
    assertEquals(BigDecimal.ONE, value.idealMeasurement());
  }

  @Test
  void bits() {
    BigInteger bit5566 = DataUnit.Bit.of(5566).bits();

    assertEquals(BigInteger.valueOf(5566), bit5566);
  }

  @Test
  void compare() {
    assertEquals(DataUnit.KB.of(1000), DataUnit.MB.of(1));
    assertNotEquals(DataUnit.KB.of(1000), DataUnit.KiB.of(1000));

    assertTrue(DataUnit.KB.of(999).smallerEqualTo(DataUnit.MB.of(1)));
    assertTrue(DataUnit.KB.of(1000).smallerEqualTo(DataUnit.MB.of(1)));
    assertTrue(DataUnit.KB.of(1000).greaterEqualTo(DataUnit.MB.of(1)));
    assertTrue(DataUnit.KB.of(1001).greaterThan(DataUnit.MB.of(1)));
    assertFalse(DataUnit.MB.of(1).smallerEqualTo(DataUnit.KB.of(999)));
    assertFalse(DataUnit.MB.of(2).smallerEqualTo(DataUnit.KB.of(1000)));
    assertFalse(DataUnit.MB.of(0).greaterEqualTo(DataUnit.KB.of(1000)));
    assertFalse(DataUnit.MB.of(1).greaterThan(DataUnit.KB.of(1001)));
  }

  @ParameterizedTest
  @CsvSource(
      delimiterString = ",",
      value = {
        // measurement, unit, expected-value
        "            1,  KiB,           1024",
        "         1000,   GB,  1000000000000",
        "            8,  Bit,              1",
        "            7,  Bit,              0",
        "            0,  Bit,              0",
        "            9,  Bit,              1",
        "           15,  Bit,              1",
        "           16,  Bit,              2",
        "          800,  Bit,            100"
      })
  void bytes(long measurement, DataUnit unit, long expected) {
    var dataSize = unit.of(measurement);

    Assertions.assertEquals(expected, dataSize.bytes());

    // overflow cases
    Assertions.assertThrows(ArithmeticException.class, () -> DataUnit.PiB.of(8192).bytes());
    Assertions.assertDoesNotThrow(() -> DataUnit.PiB.of(8191).bytes());
    Assertions.assertDoesNotThrow(() -> DataUnit.Byte.of(Long.MAX_VALUE).bytes());
  }

  @Test
  void addBytes() {
    Assertions.assertEquals(1100, DataUnit.Byte.of(1000).add(100).bytes());
    Assertions.assertEquals(1124, DataUnit.KiB.of(1).add(100).bytes());
  }

  @Test
  void subtractBytes() {
    Assertions.assertEquals(900, DataUnit.Byte.of(1000).subtract(100).bytes());
    Assertions.assertEquals(924, DataUnit.KiB.of(1).subtract(100).bytes());
  }

  @Test
  void zero() {
    Assertions.assertEquals(DataUnit.Bit.of(0), DataSize.ZERO);
    Assertions.assertEquals(0, DataSize.ZERO.bits().longValue());
  }
}
