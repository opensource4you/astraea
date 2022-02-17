package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

/** Randomly generate a long number with respect to some distribution */
public interface Distribution {
  String RANGE = "range";
  String VALUE = "value";

  long get();

  default Distribution configure(Map<String, String> parameters) {
    return this;
  }

  class DistributionConverter implements IStringConverter<Distribution> {
    @Override
    public Distribution convert(String rawArgument) {
      var args = rawArgument.split(":");
      var name = rawArgument.split(":")[0];

      switch (name) {
        case "fixed":
          if (args.length > 1) return fixed(Long.parseLong(args[1]));
          return fixed();
        case "uniform":
          if (args.length > 1) return uniform(Integer.parseInt(args[1]));
          return uniform();
        case "latest":
          return latest();
        case "zipfian":
          if (args.length > 1) return zipfian(Integer.parseInt(args[1]));
          return zipfian(30);
        default:
          throw new ParameterException(
              "Unknown distribution \"" + name + "\". use \"uniform\", \"latest\", \"zipfian\".");
      }
    }
  }

  static Distribution fixed() {
    var rand = new Random();
    return fixed(rand.nextLong());
  }

  static Distribution fixed(long value) {
    return new Distribution() {
      private long val = value;

      @Override
      public long get() {
        return val;
      }

      @Override
      public Distribution configure(Map<String, String> parameters) {
        if (parameters.isEmpty())
          throw new IllegalArgumentException("Parameter for `Distribution` should not be empty");
        else this.val = Long.parseLong(parameters.get(VALUE));
        return this;
      }
    };
  }

  /** A distribution for providing a random long number from range [0, 2147483647) */
  static Distribution uniform() {
    return uniform(Integer.MAX_VALUE);
  }

  /** A distribution for providing a random long number from range [0, N) */
  static Distribution uniform(int range) {
    var rand = new Random();
    return new Distribution() {
      private int N = range;

      @Override
      public long get() {
        return rand.nextInt(N);
      }

      @Override
      public Distribution configure(Map<String, String> parameters) {
        if (parameters.isEmpty())
          throw new IllegalArgumentException("Parameter for `Distribution` should not be empty");
        else this.N = Integer.parseInt(parameters.get(RANGE));
        return this;
      }
    };
  }

  /** A distribution for providing different random value every 2 seconds */
  static Distribution latest() {
    var rand = new Random();
    return new Distribution() {
      private long start = System.currentTimeMillis();
      private long latest = rand.nextLong();

      @Override
      public long get() {
        if (System.currentTimeMillis() - start >= 2000L) {
          latest = rand.nextLong();
          start = System.currentTimeMillis();
        }
        return latest;
      }
    };
  }

  /**
   * Building a zipfian distribution with PDF: 1/k/H_N, where H_N is the Nth harmonic number (= 1/1
   * + 1/2 + ... + 1/N); k is the key id
   */
  static Distribution zipfian(int value) {
    var rand = new Random();
    return new Distribution() {
      private List<Double> cumulativeDensityTable =
          Distribution.zipfianCumulativeDensityTable(value);

      @Override
      public long get() {
        final double randNum = rand.nextDouble();
        for (int i = 0; i < cumulativeDensityTable.size(); ++i) {
          if (randNum < cumulativeDensityTable.get(i)) return i;
        }
        return (long) cumulativeDensityTable.size() - 1L;
      }

      @Override
      public Distribution configure(Map<String, String> parameters) {
        if (parameters.isEmpty())
          throw new IllegalArgumentException("Parameter for `Distribution` should not be empty");
        cumulativeDensityTable =
            Distribution.zipfianCumulativeDensityTable(Integer.parseInt(parameters.get(RANGE)));
        return this;
      }
    };
  }

  private static List<Double> zipfianCumulativeDensityTable(int range) {
    final List<Double> cumulativeDensityTable = new ArrayList<>();
    var H_N = IntStream.range(1, range + 1).mapToDouble(k -> 1D / k).sum();
    cumulativeDensityTable.add(1D / H_N);
    IntStream.range(1, range)
        .forEach(
            i ->
                cumulativeDensityTable.add(cumulativeDensityTable.get(i - 1) + 1D / (i + 1) / H_N));
    return cumulativeDensityTable;
  }
}
