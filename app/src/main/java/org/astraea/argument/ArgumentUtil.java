package org.astraea.argument;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * A tool used to parse command line arguments.
 *
 * To add new option, add new option in the corresponding file.
 * @Parameter(names={"--option"}, description="")
 * public <optionType> <optionName>;
 * */

public class ArgumentUtil {
  // Do not instantiate.
  private ArgumentUtil() {}

  /**
   * Side effect: parse args into toolArgument
   *
   * @param toolArgument An argument object that the user want.
   * @param args Command line arguments that are put into main function.
   */
  public static <T> T parseArgument(T toolArgument, String[] args) {
    JCommander jc = JCommander.newBuilder().addObject(toolArgument).build();
    jc.setUsageFormatter(new UnixStyleUsageFormatter(jc));
    try {
      jc.parse(args);
    } catch (ParameterException pe) {
      var sb = new StringBuilder();
      jc.getUsageFormatter().usage(sb);
      throw new ParameterException(pe.getMessage() + "\n" + sb);
    }
    return toolArgument;
  }

  /* Validate Classes */
  public static class NotEmptyString implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || value.equals(""))
        throw new ParameterException(name + " should not be empty.");
    }
  }

  public static class PositiveLong implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (Long.parseLong(value) <= 0) throw new ParameterException(name + " should be positive.");
    }
  }

  public static class NonNegativeLong implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (Long.parseLong(value) < 0)
        throw new ParameterException(name + " should not be negative.");
    }
  }

  public static class PositiveDouble implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (Double.parseDouble(value) < 0D)
        throw new ParameterException(name + " should not be negative.");
    }
  }

  /* Converter classes */
  public static class StringSetConverter implements IStringConverter<Set<String>> {
    @Override
    public Set<String> convert(String value) {
      return requireNonEmpty(Set.of(value.split(",")));
    }
  }

  public static class IntegerSetConverter implements IStringConverter<Set<Integer>> {
    @Override
    public Set<Integer> convert(String value) {
      return requireNonEmpty(
          Stream.of(value.split(",")).map(Integer::valueOf).collect(Collectors.toSet()));
    }
  }

  public static class BooleanConverter implements IStringConverter<Boolean> {
    @Override
    public Boolean convert(String value) {
      return Boolean.valueOf(value);
    }
  }

  public static class ShortConverter implements IStringConverter<Short> {
    @Override
    public Short convert(String value) {
      return Short.valueOf(value);
    }
  }

  /**
   * A converter for time unit.
   *
   * <p>This converter is able to transform following time string into corresponding {@link
   * Duration}.
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
  public static class DurationConverter implements IStringConverter<Duration>, IParameterValidator {

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
    public void validate(String name, String value) throws ParameterException {
      if (!TIME_PATTERN.matcher(value).find())
        throw new ParameterException(
            "field \"" + name + "\"'s value \"" + value + "\" doesn't match time format");
    }
  }

  private static <C extends Collection<T>, T> C requireNonEmpty(C collection) {
    if (collection.isEmpty()) throw new ParameterException("array type can't be empty");
    return collection;
  }
}
