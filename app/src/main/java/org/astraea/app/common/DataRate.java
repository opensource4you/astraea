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
package org.astraea.app.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Locale;

/** Data rate class */
public class DataRate {
  private final DataSize totalBitTransmitted;
  private final BigDecimal durationInNanoSecond;

  static BigDecimal fromDurationToBigDecimalSafely(Duration duration) {
    // It the given duration is extremely long(like 1000 years), it might overflow the long
    // maximum value in nano second unit.
    if (duration.toSeconds() >= Long.MAX_VALUE / 1e9) {
      return BigDecimal.valueOf(duration.toSeconds())
          .multiply(BigDecimal.valueOf(ChronoUnit.SECONDS.getDuration().toNanos()))
          .add(BigDecimal.valueOf(duration.toNanosPart()));
    } else {
      return BigDecimal.valueOf(duration.toNanos());
    }
  }

  static BigInteger fromDurationToBigIntegerSafely(Duration duration) {
    // It the given duration is extremely long(like 1000 years), it might overflow the long
    // maximum value in nano second unit.
    if (duration.toSeconds() >= Long.MAX_VALUE / 1e9) {
      return BigInteger.valueOf(duration.toSeconds())
          .multiply(BigInteger.valueOf(ChronoUnit.SECONDS.getDuration().toNanos()))
          .add(BigInteger.valueOf(duration.toNanosPart()));
    } else {
      return BigInteger.valueOf(duration.toNanos());
    }
  }

  DataRate(DataSize initialSize, Duration duration) {
    this.totalBitTransmitted = initialSize;
    this.durationInNanoSecond = fromDurationToBigDecimalSafely(duration);
  }

  /** Return the ideal data rate under specific time unit. */
  public BigDecimal idealDataRate(ChronoUnit chronoUnit) {
    return toBigDecimal(idealDataUnit(chronoUnit), chronoUnit);
  }

  /** Return the ideal data unit under specific time unit. */
  public DataUnit idealDataUnit(ChronoUnit chronoUnit) {
    final BigInteger dataRate =
        totalBitTransmitted
            .bits()
            .multiply(fromDurationToBigIntegerSafely(chronoUnit.getDuration()))
            .divide(durationInNanoSecond.toBigInteger());

    return DataUnit.BYTE_UNIT_SIZE_ORDERED_LIST.stream()
        .sorted(Comparator.reverseOrder())
        .dropWhile((x) -> dataRate.compareTo(x.bits) < 0)
        .findFirst()
        .orElse(DataUnit.Byte);
  }

  /** Calculate the data rate in a specific {@link DataUnit} over a specific {@link ChronoUnit}. */
  public BigDecimal toBigDecimal(DataUnit dataUnit, ChronoUnit chronoUnit) {
    return toBigDecimal(dataUnit, chronoUnit.getDuration());
  }

  /** Calculate the data rate in a specific {@link DataUnit} over a specific {@link Duration}. */
  public BigDecimal toBigDecimal(DataUnit dataUnit, Duration duration) {
    return this.totalBitTransmitted
        .measurement(dataUnit)
        .multiply(fromDurationToBigDecimalSafely(duration))
        .divide(durationInNanoSecond, MathContext.DECIMAL128);
  }

  /**
   * @return the data rate with bytes/second unit as a double value. If the value is beyond the
   *     range of double, it will become {@link Double#POSITIVE_INFINITY}.
   */
  public double doubleByteRate() {
    return toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS).doubleValue();
  }

  /**
   * @return the data rate with bytes/second unit as a long value
   * @throws ArithmeticException if the value overflowed.
   */
  public long longByteRate() {
    return toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS).toBigInteger().longValueExact();
  }

  /** @return the data rate per second as a {@link DataSize}. */
  public DataSize dataSize() {
    var bitsPerSecond = toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger();
    return new DataSize(bitsPerSecond);
  }

  /**
   * Create a {@link DataRate} object from data volume and time unit
   *
   * <pre>{@code
   * DataRate.of(100, DataUnit.KB, ChronoUnit.SECONDS);    // 100 KB/s
   * DataRate.of(100, DataUnit.MB, ChronoUnit.MONTHS);     // 50 MB/month
   * }</pre>
   *
   * @return an object {@link DataRate} correspond to given arguments
   */
  public static DataRate of(long measurement, DataUnit dataUnit, ChronoUnit chronoUnit) {
    return of(measurement, dataUnit, chronoUnit.getDuration());
  }

  /**
   * Create a {@link DataRate} object from data volume and time duration
   *
   * <pre>{@code
   * DataRate.of(100, DataUnit.KB, Duration.ofSeconds(1)); // 100 KB/s
   * DataRate.of(100, DataUnit.MB, Duration.ofSeconds(2)); // 50 MB/s
   * }</pre>
   *
   * @return an object {@link DataRate} correspond to given arguments
   */
  public static DataRate of(long measurement, DataUnit dataUnit, Duration duration) {
    return new DataRate(dataUnit.of(measurement), duration);
  }

  /**
   * Create a {@link DataRate} object from data volume and time duration
   *
   * <pre>{@code
   * DataRate.of(DataUnit.KB.of(100), ChronoUnit.SECONDS); // 100 KB/s
   * DataRate.of(DataUnit.MB.of(100), ChronoUnit.SECONDS); // 50 MB/s
   * }</pre>
   *
   * @return an object {@link DataRate} correspond to given arguments
   */
  public static DataRate of(DataSize dataSize, ChronoUnit chronoUnit) {
    return new DataRate(dataSize, chronoUnit.getDuration());
  }

  /**
   * Create a {@link DataRate} object from data volume and time duration
   *
   * <pre>{@code
   * DataRate.of(DataUnit.KB.of(100), Duration.ofSeconds(1)); // 100 KB/s
   * DataRate.of(DataUnit.MB.of(100), Duration.ofSeconds(2)); // 50 MB/s
   * }</pre>
   *
   * @return an object {@link DataRate} correspond to given arguments
   */
  public static DataRate of(DataSize dataSize, Duration duration) {
    return new DataRate(dataSize, duration);
  }

  public static BigDecimal ofBigDecimal(long measurement, DataUnit unit, ChronoUnit time) {
    return ofBigDecimal(unit.of(measurement), unit, time.getDuration());
  }

  public static BigDecimal ofBigDecimal(long measurement, DataUnit unit, Duration time) {
    return ofBigDecimal(unit.of(measurement), unit, time);
  }

  public static BigDecimal ofBigDecimal(DataSize size, DataUnit unit, ChronoUnit time) {
    return ofBigDecimal(size, unit, time.getDuration());
  }

  public static BigDecimal ofBigDecimal(DataSize size, DataUnit unit, Duration time) {
    return of(size, time).toBigDecimal(unit, time);
  }

  public static double ofDouble(long measurement, DataUnit unit, ChronoUnit time) {
    return ofBigDecimal(measurement, unit, time).doubleValue();
  }

  public static double ofDouble(long measurement, DataUnit unit, Duration time) {
    return ofBigDecimal(measurement, unit, time).doubleValue();
  }

  public static double ofDouble(DataSize size, DataUnit unit, ChronoUnit time) {
    return ofBigDecimal(size, unit, time).doubleValue();
  }

  public static double ofDouble(DataSize size, DataUnit unit, Duration time) {
    return ofBigDecimal(size, unit, time).doubleValue();
  }

  /**
   * @param bytesPerSecond the double value that represent a data rate in bytes/second unit
   * @return a {@link DataRate} converted from the given parameter.
   */
  public static DataRate fromDouble(double bytesPerSecond) {
    var bits =
        BigDecimal.valueOf(bytesPerSecond)
            .multiply(new BigDecimal(DataUnit.Byte.bits))
            .toBigInteger();
    var size = new DataSize(bits);
    return new DataRate(size, Duration.ofSeconds(1));
  }

  /**
   * @param bytesPerSecond the double value that represent a data rate in bytes/second unit
   * @return a {@link DataRate} converted from the given parameter.
   */
  public static DataRate fromLong(long bytesPerSecond) {
    var bits =
        BigDecimal.valueOf(bytesPerSecond)
            .multiply(new BigDecimal(DataUnit.Byte.bits))
            .toBigInteger();
    var size = new DataSize(bits);
    return new DataRate(size, Duration.ofSeconds(1));
  }

  private static String chronoName(ChronoUnit chronoUnit) {
    return chronoUnit.name().toLowerCase(Locale.ROOT);
  }

  public String toString(DataUnit dataUnit, ChronoUnit chronoUnit) {
    BigDecimal dataRate = toBigDecimal(dataUnit, chronoUnit).setScale(2, RoundingMode.HALF_EVEN);
    return String.format("%s %s/%s", dataRate, dataUnit.name(), chronoName(chronoUnit));
  }

  public String toString(ChronoUnit chronoUnit) {
    return toString(idealDataUnit(chronoUnit), chronoUnit);
  }

  @Override
  public String toString() {
    return toString(ChronoUnit.SECONDS);
  }
}
