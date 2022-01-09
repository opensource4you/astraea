package org.astraea.automation;

import com.beust.jcommander.Parameter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.astraea.argument.ArgumentUtil;
import org.astraea.performance.Performance;

/**
 * By configuring the parameters in config/automation.properties, control the execution times of
 * performance and its configuration parameters.
 *
 * <ol>
 *   <li>--file: The address of the automation.properties in config folder.
 * </ol>
 */
public class Automation {
  private static final List<String> performanceProperties =
      List.of(
          "--bootstrap.servers",
          "--record.size",
          "--run.until",
          "--compression",
          "--consumers",
          "--fixed.size",
          "--jmx.servers",
          "--partitioner",
          "--partitions",
          "--producers",
          "--prop.file",
          "--replicas",
          "--topic");

  public static void main(String[] args) {
    try {
      var properties = new Properties();
      var arg = ArgumentUtil.parseArgument(new automationArgument(), args);
      properties.load(new FileInputStream(arg.address));

      var i = 0;
      var times = 0;
      if (properties.getProperty("--time").equals("Default")) times = 5;
      else times = Integer.parseInt(properties.getProperty("--time"));

      while (i < times) {
        var str =
            Performance.execute(
                ArgumentUtil.parseArgument(
                    new Performance.Argument(), performanceArgs(properties)));
        i++;
        System.out.println("=============== " + i + " time " + str.get() + " ===============");
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unChecked")
  private static String[] performanceArgs(Properties properties) {
    var args = new ArrayList<String>();
    performanceProperties.forEach(
        str -> {
          var property = properties.getProperty(str);
          if (property != null && !property.equals("Default")) {
            args.add(str);
            args.add(property);
          }
        });
    var strings = new String[args.size()];
    return args.toArray(strings);
  }

  private static class automationArgument {
    @Parameter(
        names = {"--file"},
        description = "String: automation.properties address",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String address = "";
  }
}
