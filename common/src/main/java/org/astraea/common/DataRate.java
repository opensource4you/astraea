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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Objects;

/** Data rate class */
public class DataRate {

  private final DataSize totalBitTransmitted;
  private final BigDecimal durationInNanoSecond;

  public static final LongSource Bit = (x) -> DataSize.Bit.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Kb = (x) -> DataSize.Kb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Mb = (x) -> DataSize.Mb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Gb = (x) -> DataSize.Gb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Tb = (x) -> DataSize.Tb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Pb = (x) -> DataSize.Pb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Eb = (x) -> DataSize.Eb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Yb = (x) -> DataSize.Yb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Zb = (x) -> DataSize.Zb.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Kib = (x) -> DataSize.Kib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Mib = (x) -> DataSize.Mib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Gib = (x) -> DataSize.Gib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Tib = (x) -> DataSize.Tib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Pib = (x) -> DataSize.Pib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Eib = (x) -> DataSize.Eib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Yib = (x) -> DataSize.Yib.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource Zib = (x) -> DataSize.Zib.of(x).dataRate(Duration.ofSeconds(1));

  public static final LongSource Byte = (x) -> DataSize.Byte.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource KB = (x) -> DataSize.KB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource MB = (x) -> DataSize.MB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource GB = (x) -> DataSize.GB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource TB = (x) -> DataSize.TB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource PB = (x) -> DataSize.PB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource EB = (x) -> DataSize.EB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource YB = (x) -> DataSize.YB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource ZB = (x) -> DataSize.ZB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource KiB = (x) -> DataSize.KiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource MiB = (x) -> DataSize.MiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource GiB = (x) -> DataSize.GiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource TiB = (x) -> DataSize.TiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource PiB = (x) -> DataSize.PiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource EiB = (x) -> DataSize.EiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource YiB = (x) -> DataSize.YiB.of(x).dataRate(Duration.ofSeconds(1));
  public static final LongSource ZiB = (x) -> DataSize.ZiB.of(x).dataRate(Duration.ofSeconds(1));

  public static final DataRate ZERO = DataRate.Byte.of(0);

  static BigDecimal fromDurationToBigDecimalSafely(Duration duration) {
    // It the given duration is extremely long(like 1000 years), it might overflow the long
    // maximum value in nanosecond unit.
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
    // maximum value in nanosecond unit.
    if (duration.toSeconds() >= Long.MAX_VALUE / 1e9) {
      return BigInteger.valueOf(duration.toSeconds())
          .multiply(BigInteger.valueOf(ChronoUnit.SECONDS.getDuration().toNanos()))
          .add(BigInteger.valueOf(duration.toNanosPart()));
    } else {
      return BigInteger.valueOf(duration.toNanos());
    }
  }

  public DataRate(DataSize initialSize, Duration duration) {
    this.totalBitTransmitted = initialSize;
    this.durationInNanoSecond = fromDurationToBigDecimalSafely(duration);
  }

  /** Return the ideal data unit under specific time unit. */
  public DataUnit idealDataUnit(Duration duration) {
    final BigInteger dataRate =
        totalBitTransmitted
            .bits()
            .multiply(fromDurationToBigIntegerSafely(duration))
            .divide(durationInNanoSecond.toBigInteger());

    return DataUnit.BYTE_UNIT_SIZE_ORDERED_LIST.stream()
        .sorted(Comparator.reverseOrder())
        .dropWhile((x) -> dataRate.compareTo(x.bits) < 0)
        .findFirst()
        .orElse(DataUnit.Byte);
  }

  /** Calculate the data rate in a specific {@link DataUnit} over a specific {@link Duration}. */
  BigDecimal toBigDecimal(DataUnit dataUnit, Duration duration) {
    return this.totalBitTransmitted
        .measurement(dataUnit)
        .multiply(fromDurationToBigDecimalSafely(duration))
        .divide(durationInNanoSecond, MathContext.DECIMAL128);
  }

  /**
   * @return the data rate with bytes/second unit as a double value. If the value is beyond the
   *     range of double, {@link Double#POSITIVE_INFINITY} will be returned.
   */
  public double byteRate() {
    return toBigDecimal(DataUnit.Byte, Duration.ofSeconds(1)).doubleValue();
  }

  /**
   * @return the data rate per second as a {@link DataSize}.
   */
  public DataSize dataSize() {
    var bitsPerSecond = toBigDecimal(DataUnit.Bit, Duration.ofSeconds(1)).toBigInteger();
    return new DataSize(bitsPerSecond);
  }

  @Override
  public String toString() {
    var dataUnit = idealDataUnit(Duration.ofSeconds(1));
    BigDecimal dataRate =
        toBigDecimal(dataUnit, Duration.ofSeconds(1)).setScale(2, RoundingMode.HALF_EVEN);
    return String.format("%s %s/%s", dataRate, dataUnit.name(), "SECOND");
  }

  @FunctionalInterface
  public interface LongSource {
    DataRate of(long measurement);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataRate dataRate = (DataRate) o;
    return Objects.equals(totalBitTransmitted, dataRate.totalBitTransmitted)
        && Objects.equals(durationInNanoSecond, dataRate.durationInNanoSecond);
  }

  @Override
  public int hashCode() {
    return Objects.hash(totalBitTransmitted, durationInNanoSecond);
  }
}
