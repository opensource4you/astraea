package org.astraea.argument;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;

/*
 * A tool used to parse command line arguments.
 *
 * To add new option, add new option in the corresponding file.
 * @Parameter(names={"--option"}, description="")
 * public <optionType> <optionName>;
 * */

public class ArgumentUtil {
  // Do not instantiate.
  private ArgumentUtil() {}

  /**
   * Side effect: parse args into toolArgument
   *
   * @param toolArgument An argument object that the user want.
   * @param args Command line arguments that are put into main function.
   */
  public static <T> T parseArgument(T toolArgument, String[] args) {
    JCommander jc = JCommander.newBuilder().addObject(toolArgument).build();
    jc.setUsageFormatter(new UnixStyleUsageFormatter(jc));
    try {
      jc.parse(args);
    } catch (ParameterException pe) {
      var sb = new StringBuilder();
      jc.getUsageFormatter().usage(sb);
      throw new ParameterException(pe.getMessage() + "\n" + sb);
    }
    return toolArgument;
  }
}
