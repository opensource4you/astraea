package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/** Randomly generate a long number with respect to some distribution */
public interface Distribution {

  long get();

  class DistributionConverter implements IStringConverter<Distribution> {
    @Override
    public Distribution convert(String rawArgument) {
      var args = rawArgument.split(":");
      var name = rawArgument.split(":")[0];

      switch (name) {
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

  static Distribution uniform() {
    return uniform(Integer.MAX_VALUE);
  }

  /** Provide a random long number */
  static Distribution uniform(int N) {
    var rand = new Random();
    return () -> (long) (rand.nextInt(N));
  }

  /** Provide different value every 2 seconds */
  static Distribution latest() {
    return () -> System.currentTimeMillis() / 2000L;
  }

  /**
   * Building a zipfian distribution with PDF: 1/k/H_N, where H_N is the Nth harmonic number; k is
   * the key id
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
