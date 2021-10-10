package org.astraea.argument;

import com.beust.jcommander.Parameter;
import java.util.Map;

public abstract class BasicAdminArgument extends BasicArgument {
  @Parameter(
      names = {"--admin.props.file"},
      description = "the file path containing the properties to be passed to kafka admin",
      validateWith = ArgumentUtil.NotEmptyString.class)
  public String propsFile;

  /**
   * @return the kafka props consists of bootstrap servers and all props from file (if it is
   *     existent)
   */
  public Map<String, Object> adminProps() {
    return properties(propsFile);
  }
}
