package org.astraea.argument;

import com.beust.jcommander.ParameterException;

public interface PositiveNumberField<T extends Number> extends Field<T> {

  @Override
  default void validate(String name, String value) throws ParameterException {
    if (Long.parseLong(value) <= 0) throw new ParameterException(name + " should be positive.");
  }
}
