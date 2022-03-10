package org.astraea.automation;

import com.beust.jcommander.Parameter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.astraea.argument.NonEmptyStringField;
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
      var arg = org.astraea.argument.Argument.parse(new Argument(), args);
      properties.load(new FileInputStream(arg.address));
      var whetherDeleteTopic = properties.getProperty("--whetherDeleteTopic").equals("true");
      var bootstrap = properties.getProperty("--bootstrap.servers");
      var config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

      var i = 0;
      var times = 0;
      if (properties.getProperty("--time").equals("Default")) times = 5;
      else times = Integer.parseInt(properties.getProperty("--time"));

      while (i < times) {
        var result =
            Performance.execute(
                org.astraea.argument.Argument.parse(
                    new Performance.Argument(), performanceArgs(properties)));
        i++;
        if (whetherDeleteTopic) {
          try (final AdminClient adminClient = KafkaAdminClient.create(config)) {
            adminClient.deleteTopics(List.of(result.topicName()));
          }
        }
        System.out.println("=============== " + i + " time Performance Complete! ===============");
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

  private static class Argument {
    @Parameter(
        names = {"--file"},
        description = "String: automation.properties address",
        validateWith = NonEmptyStringField.class)
    String address = "";
  }
}
