package org.astraea.utils;

import com.beust.jcommander.IStringConverter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Data size class */
public class DataSize implements Comparable<DataSize> {

  private final BigInteger bits;

  // Parse number and DataUnit
  private static final Pattern DATA_SIZE_PATTERN =
      Pattern.compile("(?<measurement>[0-9]+)\\s?(?<dataUnit>[a-zA-Z]+)");

  DataSize(long volume, DataUnit dataUnit) {
    this(BigInteger.valueOf(volume).multiply(dataUnit.bits));
  }

  DataSize(BigInteger bigInteger) {
    this.bits = bigInteger;
  }

  /**
   * Convert string to DataSize.
   *
   * <pre>{@code
   * DataSize.parseDataSize("500KB");  // 500 KB  (500 * 1000 bytes)
   * DataSize.parseDataSize("500KiB"); // 500 KiB (500 * 1024 bytes)
   * DataSize.parseDataSize("500Kb");  // 500 Kb  (500 * 1000 bits)
   * DataSize.parseDataSize("500Kib"); // 500 Kib (500 * 1024 bits)
   * }</pre>
   *
   * @param argument number and the unit. e.g. "500MiB", "9876 KB"
   * @return a data size object of given measurement under specific data unit.
   */
  public static DataSize parseDataSize(String argument) {
    Matcher matcher = DATA_SIZE_PATTERN.matcher(argument);
    if (matcher.matches()) {
      return DataUnit.valueOf(matcher.group("dataUnit"))
          .of(Long.parseLong(matcher.group("measurement")));
    } else {
      throw new IllegalArgumentException("Unknown DataSize \"" + argument + "\"");
    }
  }

  /**
   * Add data volume.
   *
   * @param measurement data measurement.
   * @param dataUnit data unit.
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize add(long measurement, DataUnit dataUnit) {
    return add(new DataSize(measurement, Objects.requireNonNull(dataUnit)));
  }

  /**
   * Subtract data volume.
   *
   * @param measurement data measurement.
   * @param dataUnit data unit.
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize subtract(long measurement, DataUnit dataUnit) {
    return subtract(new DataSize(measurement, Objects.requireNonNull(dataUnit)));
  }

  /**
   * Multiply current data volume by a scalar.
   *
   * @param scalar a long integer.
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize multiply(long scalar) {
    return multiply(BigInteger.valueOf(scalar));
  }

  /**
   * Divide current data volume by a scalar.
   *
   * @param scalar a long integer.
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize divide(long scalar) {
    return divide(BigInteger.valueOf(scalar));
  }

  /**
   * Add by given {@link DataSize}.
   *
   * @param rhs the right hand side value.
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize add(DataSize rhs) {
    return add(Objects.requireNonNull(rhs).bits);
  }

  /**
   * Subtract by given {@link DataSize}.
   *
   * @param rhs the right hand size value.
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize subtract(DataSize rhs) {
    return subtract(Objects.requireNonNull(rhs).bits);
  }

  public boolean smallerThan(DataSize dataSize) {
    return this.compareTo(dataSize) < 0;
  }

  public boolean smallerEqualTo(DataSize dataSize) {
    return this.compareTo(dataSize) <= 0;
  }

  public boolean greaterThan(DataSize dataSize) {
    return this.compareTo(dataSize) > 0;
  }

  public boolean greaterEqualTo(DataSize dataSize) {
    return this.compareTo(dataSize) >= 0;
  }

  private DataSize add(BigInteger rhs) {
    return new DataSize(bits.add(rhs));
  }

  private DataSize subtract(BigInteger rhs) {
    return new DataSize(bits.subtract(rhs));
  }

  private DataSize multiply(BigInteger rhs) {
    return new DataSize(bits.multiply(rhs));
  }

  private DataSize divide(BigInteger rhs) {
    return new DataSize(bits.divide(rhs));
  }

  /** Current bits. */
  public BigInteger bits() {
    return bits;
  }

  /**
   * The measurement value in term of specific data unit.
   *
   * @param dataUnit data unit to describe current size.
   * @return a {@link BigDecimal} describe current data size in term of specific data unit.
   */
  public BigDecimal measurement(DataUnit dataUnit) {
    return new BigDecimal(this.bits).divide(new BigDecimal(dataUnit.bits), MathContext.DECIMAL32);
  }

  /**
   * The measurement value in term of the most ideal data unit for human readability.
   *
   * @return a {@link BigDecimal} describe current data size in term of the ideal data unit.
   */
  public BigDecimal idealMeasurement() {
    return measurement(idealDataUnit());
  }

  /**
   * Return the most ideal data unit in terms of human readability. Usually, the most suitable unit
   * describes the data volume in the fewest decimal digits.
   *
   * <p>For example, given 1,000,000,000 bytes. We can describe this data volume in various way:
   *
   * <ol>
   *   <li>8,000,000,000 Bit
   *   <li>1,000,000,000 Byte
   *   <li>1,000,000 KB
   *   <li>1,000 MB
   *   <li>1 GB
   *   <li>0.0001 TB
   * </ol>
   *
   * <p>For all these possibilities, GB is more ideal since it has the fewest decimal digits.
   *
   * @return a {@link DataUnit} that is suitable to describe current data volume.
   */
  public DataUnit idealDataUnit() {
    return DataUnit.BYTE_UNIT_SIZE_ORDERED_LIST.stream()
        .sorted(Comparator.reverseOrder())
        .dropWhile((x) -> this.bits.compareTo(x.bits) < 0)
        .findFirst()
        .orElse(DataUnit.Byte);
  }

  /** Return a {@link DataRate} based on current data size over a specific time unit. */
  public DataRate dataRate(ChronoUnit chronoUnit) {
    return DataRate.of(this, chronoUnit.getDuration());
  }

  /**
   * Return a {@link DataRate} based on current data size over a specific {@link Duration} of time.
   */
  public DataRate dataRate(Duration timePassed) {
    return DataRate.of(this, timePassed);
  }

  /** Return a string represent current size in given data unit. */
  public String toString(DataUnit unit) {
    return String.format("%s %s", this.bits.divide(unit.bits), unit.name());
  }

  /**
   * Return a string representing the current size, the string will use an easy to read data unit.
   */
  @Override
  public String toString() {
    return toString(idealDataUnit());
  }

  @Override
  public int compareTo(DataSize hs) {
    return this.bits.compareTo(hs.bits);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataSize size = (DataSize) o;
    return bits.equals(size.bits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bits);
  }

  public static class Converter implements IStringConverter<DataSize> {
    public DataSize convert(String argument) {
      return DataSize.parseDataSize(argument);
    }
  }
}
