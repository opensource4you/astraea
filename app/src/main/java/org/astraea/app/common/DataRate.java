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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Data rate class */
public class DataRate {
  private final DataSize totalBitTransmitted;
  private final BigDecimal durationInNanoSecond;

  public static final DataSizeSource Size = DataRateBuilder::new;
  public static final LongSource Bit = (x) -> new DataRateBuilder(DataSize.Bit.of(x));
  public static final LongSource Kb = (x) -> new DataRateBuilder(DataSize.Kb.of(x));
  public static final LongSource Mb = (x) -> new DataRateBuilder(DataSize.Mb.of(x));
  public static final LongSource Gb = (x) -> new DataRateBuilder(DataSize.Gb.of(x));
  public static final LongSource Tb = (x) -> new DataRateBuilder(DataSize.Tb.of(x));
  public static final LongSource Pb = (x) -> new DataRateBuilder(DataSize.Pb.of(x));
  public static final LongSource Eb = (x) -> new DataRateBuilder(DataSize.Eb.of(x));
  public static final LongSource Yb = (x) -> new DataRateBuilder(DataSize.Yb.of(x));
  public static final LongSource Zb = (x) -> new DataRateBuilder(DataSize.Zb.of(x));
  public static final LongSource Kib = (x) -> new DataRateBuilder(DataSize.Kib.of(x));
  public static final LongSource Mib = (x) -> new DataRateBuilder(DataSize.Mib.of(x));
  public static final LongSource Gib = (x) -> new DataRateBuilder(DataSize.Gib.of(x));
  public static final LongSource Tib = (x) -> new DataRateBuilder(DataSize.Tib.of(x));
  public static final LongSource Pib = (x) -> new DataRateBuilder(DataSize.Pib.of(x));
  public static final LongSource Eib = (x) -> new DataRateBuilder(DataSize.Eib.of(x));
  public static final LongSource Yib = (x) -> new DataRateBuilder(DataSize.Yib.of(x));
  public static final LongSource Zib = (x) -> new DataRateBuilder(DataSize.Zib.of(x));

  public static final LongSource Byte = (x) -> new DataRateBuilder(DataSize.Byte.of(x));
  public static final LongSource KB = (x) -> new DataRateBuilder(DataSize.KB.of(x));
  public static final LongSource MB = (x) -> new DataRateBuilder(DataSize.MB.of(x));
  public static final LongSource GB = (x) -> new DataRateBuilder(DataSize.GB.of(x));
  public static final LongSource TB = (x) -> new DataRateBuilder(DataSize.TB.of(x));
  public static final LongSource PB = (x) -> new DataRateBuilder(DataSize.PB.of(x));
  public static final LongSource EB = (x) -> new DataRateBuilder(DataSize.EB.of(x));
  public static final LongSource YB = (x) -> new DataRateBuilder(DataSize.YB.of(x));
  public static final LongSource ZB = (x) -> new DataRateBuilder(DataSize.ZB.of(x));
  public static final LongSource KiB = (x) -> new DataRateBuilder(DataSize.KiB.of(x));
  public static final LongSource MiB = (x) -> new DataRateBuilder(DataSize.MiB.of(x));
  public static final LongSource GiB = (x) -> new DataRateBuilder(DataSize.GiB.of(x));
  public static final LongSource TiB = (x) -> new DataRateBuilder(DataSize.TiB.of(x));
  public static final LongSource PiB = (x) -> new DataRateBuilder(DataSize.PiB.of(x));
  public static final LongSource EiB = (x) -> new DataRateBuilder(DataSize.EiB.of(x));
  public static final LongSource YiB = (x) -> new DataRateBuilder(DataSize.YiB.of(x));
  public static final LongSource ZiB = (x) -> new DataRateBuilder(DataSize.ZiB.of(x));

  public static final DataRate ZERO = DataRate.Byte.of(0).perSecond();

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
   *     range of double, {@link Double#POSITIVE_INFINITY} will be returned.
   */
  public double byteRate() {
    return toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS).doubleValue();
  }

  /** @return the data rate per second as a {@link DataSize}. */
  public DataSize dataSize() {
    var bitsPerSecond = toBigDecimal(DataUnit.Bit, ChronoUnit.SECONDS).toBigInteger();
    return new DataSize(bitsPerSecond);
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

  @FunctionalInterface
  public interface LongSource {
    DataRateBuilder of(long measurement);
  }

  @FunctionalInterface
  public interface DataSizeSource {
    DataRateBuilder of(DataSize size);
  }

  public static final class DataRateBuilder {

    private final DataSize dataSize;

    DataRateBuilder(DataSize dataSize) {
      this.dataSize = dataSize;
    }

    public DataRate perSecond() {
      return dataSize.dataRate(ChronoUnit.SECONDS);
    }

    public DataRate perMinute() {
      return dataSize.dataRate(ChronoUnit.MINUTES);
    }

    public DataRate perHour() {
      return dataSize.dataRate(ChronoUnit.HOURS);
    }

    public DataRate perDay() {
      return dataSize.dataRate(ChronoUnit.DAYS);
    }

    public DataRate over(Duration duration) {
      return dataSize.dataRate(duration);
    }

    public DataRate over(ChronoUnit chronoUnit) {
      return dataSize.dataRate(chronoUnit);
    }
  }

  public static class Field extends org.astraea.app.argument.Field<DataRate> {

    static final Duration DEFAULT_DURATION = Duration.ofSeconds(1);
    private static final Pattern DATA_SIZE_PATTERN =
        Pattern.compile(
            "(?<measurement>[0-9]+)\\s?(?<dataUnit>[a-zA-Z]+)(?<duration>/[\\da-z-+.,A-Z]+)?");

    /**
     * Convert string to DataRate.
     *
     * <pre>{@code
     * new DataRate.Field().convert("500KB/s");  // 500 KB  (500 * 1000 bytes)/ per second
     * new DataRate.Field().convert("500KiB/H"); // 500 KiB (500 * 1024 bytes)/ per hour
     * new DataRate.Field().convert("500Kb/minute");  // 500 Kb  (500 * 1000 bits)/ per minute
     * new DataRate.Field().convert("500Kib/PT-20S"); // 500 Kib (500 * 1024 bits)/ per 20 second
     * }</pre>
     *
     * @param argument number and the unit and duration e.g. "500MiB/s", "9876 KB/PT-20S"
     * @return data size with data unit under duration.
     */
    @Override
    public DataRate convert(String argument) {

      Matcher matcher = DATA_SIZE_PATTERN.matcher(argument);
      if (matcher.matches()) {
        var measurement = Long.parseLong(matcher.group("measurement"));
        var dataUnit = DataUnit.valueOf(matcher.group("dataUnit"));
        var duration = getDuration(matcher.group("duration"));
        return new DataRate(dataUnit.of(measurement), duration);
      } else {
        throw new IllegalArgumentException("Unknown DataRate \"" + argument + "\"");
      }
    }

    enum CustomDurationMapping {
      SECOND(List.of("s", "second"), Duration.ofSeconds(1)),
      MINUTE(List.of("m", "minute"), Duration.ofMinutes(1)),
      HOUR(List.of("h", "hour"), Duration.ofHours(1)),
      DAY(List.of("d", "day"), Duration.ofDays(1));
      private final List<String> expression;
      private final Duration duration;

      CustomDurationMapping(List<String> expression, Duration duration) {
        this.expression = expression;
        this.duration = duration;
      }

      public List<String> expression() {
        return expression;
      }

      public Duration duration() {
        return duration;
      }
    }

    private Duration getDuration(String duration) {
      if (Objects.isNull(duration)) {
        return DEFAULT_DURATION;
      }
      var lowerCaseDuration = duration.substring(1).toLowerCase();
      return Arrays.stream(CustomDurationMapping.values())
          .filter(x -> x.expression().contains(lowerCaseDuration))
          .map(CustomDurationMapping::duration)
          .findFirst()
          .orElseGet(() -> Duration.parse(lowerCaseDuration));
    }
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
