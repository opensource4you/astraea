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
        .divide(durationInNanoSecond, MathContext.DECIMAL32);
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
