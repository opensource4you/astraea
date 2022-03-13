package org.astraea.performance;

import com.beust.jcommander.ParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.astraea.argument.Field;

/**
 * Random distribution generator. Example: {@code Supplier<long> uniformDistribution =
 * Distribution.UNIFORM.create(100); while (true){ // the value will in range [0, 100)
 * uniformDistribution.get(); } }
 */
public enum DistributionType {
  FIXED("fixed") {
    @Override
    public Supplier<Long> create(int n) {
      return () -> (long) n;
    }
  },

  UNIFORM("uniform") {
    @Override
    public Supplier<Long> create(int n) {
      var rand = new Random();
      return () -> (long) rand.nextInt(n);
    }
  },

  /** A distribution for providing different random value every 2 seconds */
  LATEST("latest") {
    @Override
    public Supplier<Long> create(int n) {
      var rand = new Random();
      return () -> {
        var time = System.currentTimeMillis();
        rand.setSeed(time - time % 2000);
        return (long) rand.nextInt(n);
      };
    }
  },

  /**
   * Building a zipfian distribution with PDF: 1/k/H_N, where H_N is the Nth harmonic number (= 1/1
   * + 1/2 + ... + 1/N); k is the key id
   */
  ZIPFIAN("zipfian") {
    @Override
    public Supplier<Long> create(int n) {
      var cumulativeDensityTable = new ArrayList<Double>();
      var rand = new Random();
      var H_N = IntStream.range(1, n + 1).mapToDouble(k -> 1D / k).sum();
      cumulativeDensityTable.add(1D / H_N);
      IntStream.range(1, n)
          .forEach(
              i ->
                  cumulativeDensityTable.add(
                      cumulativeDensityTable.get(i - 1) + 1D / (i + 1) / H_N));
      return () -> {
        final double randNum = rand.nextDouble();
        for (int i = 0; i < cumulativeDensityTable.size(); ++i) {
          if (randNum < cumulativeDensityTable.get(i)) return (long) i;
        }
        return (long) cumulativeDensityTable.size() - 1L;
      };
    }
  };

  public final String name;

  abstract Supplier<Long> create(int n);

  DistributionType(String name) {
    this.name = name;
  }

  /**
   * convert(String): Accept lower-case name only e.g. "fixed", "uniform", "latest" and "zipfian"
   * are legal e.g. "Fixed" and "UNIFORM" are illegal
   */
  static class DistributionTypeField extends Field<DistributionType> {
    @Override
    public DistributionType convert(String name) {
      return Arrays.stream(DistributionType.values())
          .filter(distribute -> distribute.name.equals(name))
          .findFirst()
          .orElseThrow(
              () ->
                  new ParameterException(
                      "Unknown distribution \""
                          + name
                          + "\". use \"fixed\" \"uniform\", \"latest\", \"zipfian\"."));
    }
  }
}
