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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Data size class */
public class DataSize implements Comparable<DataSize> {

  public static final DataSize ZERO = DataUnit.Byte.of(0);

  public static final DataSizeSource Bit = DataUnit.Bit::of;
  public static final DataSizeSource Kb = DataUnit.Kb::of;
  public static final DataSizeSource Mb = DataUnit.Mb::of;
  public static final DataSizeSource Gb = DataUnit.Gb::of;
  public static final DataSizeSource Tb = DataUnit.Tb::of;
  public static final DataSizeSource Pb = DataUnit.Pb::of;
  public static final DataSizeSource Eb = DataUnit.Eb::of;
  public static final DataSizeSource Yb = DataUnit.Yb::of;
  public static final DataSizeSource Zb = DataUnit.Zb::of;
  public static final DataSizeSource Kib = DataUnit.Kib::of;
  public static final DataSizeSource Mib = DataUnit.Mib::of;
  public static final DataSizeSource Gib = DataUnit.Gib::of;
  public static final DataSizeSource Tib = DataUnit.Tib::of;
  public static final DataSizeSource Pib = DataUnit.Pib::of;
  public static final DataSizeSource Eib = DataUnit.Eib::of;
  public static final DataSizeSource Yib = DataUnit.Yib::of;
  public static final DataSizeSource Zib = DataUnit.Zib::of;

  public static final DataSizeSource Byte = DataUnit.Byte::of;
  public static final DataSizeSource KB = DataUnit.KB::of;
  public static final DataSizeSource MB = DataUnit.MB::of;
  public static final DataSizeSource GB = DataUnit.GB::of;
  public static final DataSizeSource TB = DataUnit.TB::of;
  public static final DataSizeSource PB = DataUnit.PB::of;
  public static final DataSizeSource EB = DataUnit.EB::of;
  public static final DataSizeSource YB = DataUnit.YB::of;
  public static final DataSizeSource ZB = DataUnit.ZB::of;
  public static final DataSizeSource KiB = DataUnit.KiB::of;
  public static final DataSizeSource MiB = DataUnit.MiB::of;
  public static final DataSizeSource GiB = DataUnit.GiB::of;
  public static final DataSizeSource TiB = DataUnit.TiB::of;
  public static final DataSizeSource PiB = DataUnit.PiB::of;
  public static final DataSizeSource EiB = DataUnit.EiB::of;
  public static final DataSizeSource YiB = DataUnit.YiB::of;
  public static final DataSizeSource ZiB = DataUnit.ZiB::of;

  private final BigInteger bits;

  DataSize(long volume, DataUnit dataUnit) {
    this(BigInteger.valueOf(volume).multiply(dataUnit.bits));
  }

  DataSize(BigInteger bits) {
    this.bits = bits;
  }

  /**
   * Add data volume.
   *
   * @param bytes value to add
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize add(long bytes) {
    return add(bytes, DataUnit.Byte);
  }

  /**
   * Subtract data volume.
   *
   * @param bytes value to subtract
   * @return a new {@link DataSize} that have applied the math operation.
   */
  public DataSize subtract(long bytes) {
    return subtract(bytes, DataUnit.Byte);
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
   * @return the current bytes in long primitive type. All the remaining bits that can't become a
   *     byte are discarded from the return value.
   * @throws ArithmeticException if the value will not exactly fit into a long.
   */
  public long bytes() {
    return bits().divide(BigInteger.valueOf(8)).longValueExact();
  }

  /**
   * The measurement value in term of specific data unit.
   *
   * @param dataUnit data unit to describe current size.
   * @return a {@link BigDecimal} describe current data size in term of specific data unit.
   */
  public BigDecimal measurement(DataUnit dataUnit) {
    return new BigDecimal(this.bits).divide(new BigDecimal(dataUnit.bits), MathContext.DECIMAL128);
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
    return dataRate(chronoUnit.getDuration());
  }

  /**
   * Return a {@link DataRate} based on current data size over a specific {@link Duration} of time.
   */
  public DataRate dataRate(Duration timePassed) {
    return new DataRate(this, timePassed);
  }

  /** Return a string represent current size in given data unit. */
  public String toString(DataUnit unit) {
    var divide =
        new BigDecimal(this.bits).divide(new BigDecimal(unit.bits), 3, RoundingMode.HALF_EVEN);
    return String.format("%s %s", divide, unit.name());
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

  @FunctionalInterface
  public interface DataSizeSource {
    DataSize of(long measurement);
  }

  public static class Field extends org.astraea.app.argument.Field<DataSize> {
    // Parse number and DataUnit
    private static final Pattern DATA_SIZE_PATTERN =
        Pattern.compile("(?<measurement>[0-9]+)\\s?(?<dataUnit>[a-zA-Z]+)");

    /**
     * Convert string to DataSize.
     *
     * <pre>{@code
     * new DataSize.Field().convert("500KB");  // 500 KB  (500 * 1000 bytes)
     * new DataSize.Field().convert("500KiB"); // 500 KiB (500 * 1024 bytes)
     * new DataSize.Field().convert("500Kb");  // 500 Kb  (500 * 1000 bits)
     * new DataSize.Field().convert("500Kib"); // 500 Kib (500 * 1024 bits)
     * }</pre>
     *
     * @param argument number and the unit. e.g. "500MiB", "9876 KB"
     * @return a data size object of given measurement under specific data unit.
     */
    @Override
    public DataSize convert(String argument) {
      Matcher matcher = DATA_SIZE_PATTERN.matcher(argument);
      if (matcher.matches()) {
        return DataUnit.valueOf(matcher.group("dataUnit"))
            .of(Long.parseLong(matcher.group("measurement")));
      } else {
        throw new IllegalArgumentException("Unknown DataSize \"" + argument + "\"");
      }
    }
  }
}
