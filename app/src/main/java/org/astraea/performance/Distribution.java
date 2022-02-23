package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Random distribution generator. Example: {@code Supplier<long> uniformDistribution =
 * Distribution.UNIFORM.create(100); while (true){ // the value will in range [0, 100)
 * uniformDistribution.get(); } }
 */
public enum Distribution {
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

  Distribution(String name) {
    this.name = name;
  }

  static class Converter implements IStringConverter<Distribution> {
    @Override
    public Distribution convert(String name) {
      return Arrays.stream(Distribution.values())
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
