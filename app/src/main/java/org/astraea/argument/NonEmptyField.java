package org.astraea.argument;

import com.beust.jcommander.ParameterException;

public interface NonEmptyField<T> extends Field<T> {

  @Override
  default void validate(String name, String value) throws ParameterException {
    if (value == null || value.isBlank())
      throw new ParameterException(name + " should not be empty.");
  }
}
