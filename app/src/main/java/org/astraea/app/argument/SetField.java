package org.astraea.app.argument;

import com.beust.jcommander.ParameterException;
import java.util.Set;

public abstract class SetField<T> extends Field<Set<T>> {
  protected static final String SEPARATOR = ",";

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (value == null || value.isBlank() || Set.of(value.split(SEPARATOR)).isEmpty())
      throw new ParameterException("set type can't be empty");
  }
}
