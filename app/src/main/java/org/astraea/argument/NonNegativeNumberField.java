package org.astraea.argument;

import com.beust.jcommander.ParameterException;

public interface NonNegativeNumberField<T extends Number> extends NonEmptyField<T> {

  @Override
  default void validate(String name, String value) throws ParameterException {
    NonEmptyField.super.validate(name, value);
    if (Long.parseLong(value) < 0) throw new ParameterException(name + " should not be negative.");
  }
}
