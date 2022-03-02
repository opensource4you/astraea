package org.astraea.performance;

import com.beust.jcommander.ParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.astraea.argument.Field;

/** Randomly generate a long number with respect to some distribution */
public interface Distribution {

  long get();

  class DistributionField extends Field<Distribution> {
    @Override
    public Distribution convert(String rawArgument) {
      var args = rawArgument.split(":");
      var name = rawArgument.split(":")[0];

      switch (name) {
        case "fixed":
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
    return () -> value;
  }

  /** A distribution for providing a random long number from range [0, 2147483647) */
  static Distribution uniform() {
    return uniform(Integer.MAX_VALUE);
  }

  /** A distribution for providing a random long number from range [0, N) */
  static Distribution uniform(int N) {
    var rand = new Random();
    return () -> (long) (rand.nextInt(N));
  }

  /** A distribution for providing different value every 2 seconds */
  static Distribution latest() {
    return () -> System.currentTimeMillis() / 2000L;
  }

  /**
   * Building a zipfian distribution with PDF: 1/k/H_N, where H_N is the Nth harmonic number (= 1/1
   * + 1/2 + ... + 1/N); k is the key id
   */
  static Distribution zipfian(int N) {
    var rand = new Random();
    final List<Double> cumulativeDensityTable = new ArrayList<>();
    var H_N = IntStream.range(1, N + 1).mapToDouble(k -> 1D / k).sum();
    cumulativeDensityTable.add(1D / H_N);
    IntStream.range(1, N)
        .forEach(
            i ->
                cumulativeDensityTable.add(cumulativeDensityTable.get(i - 1) + 1D / (i + 1) / H_N));
    return () -> {
      final double randNum = rand.nextDouble();
      for (int i = 0; i < cumulativeDensityTable.size(); ++i) {
        if (randNum < cumulativeDensityTable.get(i)) return (long) i;
      }
      return (long) cumulativeDensityTable.size() - 1L;
    };
  }
}
