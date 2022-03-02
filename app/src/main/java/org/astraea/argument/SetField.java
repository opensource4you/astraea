package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import java.util.Set;

public interface SetField<T> extends Field<Set<T>> {
  String SEPARATOR = ",";

  @Override
  default void validate(String name, String value) throws ParameterException {
    if (value == null || value.isBlank() || Set.of(value.split(SEPARATOR)).isEmpty())
      throw new ParameterException("set type can't be empty");
  }
}
