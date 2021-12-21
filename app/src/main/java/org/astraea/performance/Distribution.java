package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

/** Name of distributions. */
public enum Distribution {
  NONE,
  UNIFORM,
  ZIPFIAN,
  LATEST;

  public static class DistributionConverter implements IStringConverter<Distribution> {
    @Override
    public Distribution convert(String value) {
      switch (value) {
        case "uniform":
          return UNIFORM;
        case "zipfian":
          return ZIPFIAN;
        case "latest":
          return LATEST;
        default:
          throw new ParameterException("Unknown distribution.");
      }
    }
  }
}
