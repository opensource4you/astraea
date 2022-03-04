package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A converter for time unit.
 *
 * <p>This converter is able to transform following time string into corresponding {@link Duration}.
 *
 * <ul>
 *   <li>{@code "30s"} to {@code Duration.ofSeconds(30)}
 *   <li>{@code "1m"} to {@code Duration.ofMinutes(1)}
 *   <li>{@code "24h"} to {@code Duration.ofHours(24)}
 *   <li>{@code "7day"} to {@code Duration.ofDays(7)}
 *   <li>{@code "7days"} to {@code Duration.ofDays(7)}
 *   <li>{@code "350ms"} to {@code Duration.ofMillis(350)}
 *   <li>{@code "123us"} to {@code Duration.ofNanos(123 * 1000)}
 *   <li>{@code "100ns"} to {@code Duration.ofNanos(100)}
 * </ul>
 *
 * If no unit specified, second unit will be used:
 *
 * <ul>
 *   <li>{@code "1"} to {@code Duration.ofSeconds(1)}
 *   <li>{@code "0"} to {@code Duration.ofSeconds(0)}
 * </ul>
 *
 * Currently, negative time is not supported. So the following example doesn't work.
 *
 * <ul>
 *   <li><b>(doesn't work)</b> {@code "-1" to {@code Duration.ofSeconds(-1)}}
 * </ul>
 *
 * Currently, floating value time is not supported. So the following example doesn't work.
 *
 * <ul>
 *   <li><b>(doesn't work)</b> {@code "0.5" to {@code Duration.ofMillis(500)}}
 * </ul>
 */
public class DurationField extends Field<Duration> {

  static final Pattern TIME_PATTERN =
      Pattern.compile("^(?<value>[0-9]+)(?<unit>days|day|h|m|s|ms|us|ns|)$");

  @Override
  public Duration convert(String input) {
    Matcher matcher = TIME_PATTERN.matcher(input);
    if (matcher.find()) {
      long value = Long.parseLong(matcher.group("value"));
      String unit = matcher.group("unit");
      switch (unit) {
        case "days":
        case "day":
          return Duration.ofDays(value);
        case "h":
          return Duration.ofHours(value);
        case "m":
          return Duration.ofMinutes(value);
        case "ms":
          return Duration.ofMillis(value);
        case "us":
          return Duration.ofNanos(value * 1000);
        case "ns":
          return Duration.ofNanos(value);
        case "s":
        default:
          return Duration.ofSeconds(value);
      }
    } else {
      throw new IllegalArgumentException("value \"" + input + "\" doesn't match any time format");
    }
  }

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (!TIME_PATTERN.matcher(value).find())
      throw new ParameterException(
          "field \"" + name + "\"'s value \"" + value + "\" doesn't match time format");
  }
}
