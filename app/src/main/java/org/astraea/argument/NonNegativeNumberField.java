package org.astraea.argument;

import com.beust.jcommander.ParameterException;

public abstract class NonNegativeNumberField<T extends Number> extends Field<T> {

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (Long.parseLong(value) < 0) throw new ParameterException(name + " should not be negative.");
  }
}
