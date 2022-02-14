package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import java.time.Duration;
import java.util.function.BiFunction;
import org.astraea.argument.converter.DurationConverter;

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
    final Duration duration = new DurationConverter().convert(exeTime);
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
}
