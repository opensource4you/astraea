package org.astraea.performance;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import org.astraea.argument.ArgumentUtil;

/**
 * Two kind of running modes. One runs for a duration of time. The other runs for a number of
 * records.
 */
interface ExeTime {

  double percentage(long records, long elapsedTime);

  static ExeTime of(String exeTime) {
    if (exeTime.endsWith("records")) {
      final long records = Long.parseLong(exeTime.replace("records", ""));
      return ExeTime.of((completeRecords, ignore) -> 100D * completeRecords / records, exeTime);
    }
    final Duration duration = new ArgumentUtil.DurationConverter().convert(exeTime);
    return ExeTime.of((ignore, elapsedTime) -> 100D * elapsedTime / duration.toMillis(), exeTime);
  }

  static ExeTime of(BiFunction<Long, Long, Double> function, String toString) {
    return new ExeTime() {
      @Override
      public double percentage(long records, long duration) {
        return function.apply(records, duration);
      }

      @Override
      public String toString() {
        return toString;
      }
    };
  }

  class Converter implements IStringConverter<ExeTime> {
    @Override
    public ExeTime convert(String value) {
      return ExeTime.of(value);
    }
  }

  class Validator implements IParameterValidator {
    static final Pattern PATTERN = Pattern.compile("^([0-9]+)(days|day|h|m|s|ms|us|ns|records)$");

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (!PATTERN.matcher(value).matches()) {
        throw new ParameterException(
            "Invalid ExeTime format. valid format example: \"1m20s\" or \"89242records\"");
      }
    }
  }
}
