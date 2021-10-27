package org.astraea.argument;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
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

  /* Converter classes */
  public static class DurationConverter implements IStringConverter<Duration> {
    @Override
    public Duration convert(String value) {
      return Duration.ofSeconds(Long.parseLong(value));
    }
  }

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

  private static <C extends Collection<T>, T> C requireNonEmpty(C collection) {
    if (collection.isEmpty()) throw new ParameterException("array type can't be empty");
    return collection;
  }
}
