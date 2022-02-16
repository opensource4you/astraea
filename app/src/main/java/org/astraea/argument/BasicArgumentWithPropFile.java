package org.astraea.argument;

import com.beust.jcommander.Parameter;
import java.util.Map;
import org.astraea.argument.validator.NotEmptyString;

public abstract class BasicArgumentWithPropFile extends BasicArgument {
  @Parameter(
      names = {"--prop.file"},
      description = "the file path containing the properties to be passed to kafka admin",
      validateWith = NotEmptyString.class)
  public String propFile;

  /**
   * @return the kafka props consists of bootstrap servers and all props from file (if it is
   *     existent)
   */
  public Map<String, Object> props() {
    return properties(propFile);
  }
}
