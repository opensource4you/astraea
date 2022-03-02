package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class StringMapField extends Field<Map<String, String>> {

  @Override
  public Map<String, String> convert(String value) {
    return Arrays.stream(value.split(","))
        .map(
            item -> {
              var keyValue = item.split("=");
              if (keyValue.length != 2) throw new ParameterException("incorrect format: " + item);
              return Map.entry(keyValue[0], keyValue[1]);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (!value.contains("=") || convert(value).isEmpty())
      throw new ParameterException("incorrect format: " + value);
  }
}
