package org.astraea.argument;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;

/** This basic argument defines the common property used by all kafka clients. */
public abstract class Argument {

  /**
   * Side effect: parse args into toolArgument
   *
   * @param toolArgument An argument object that the user want.
   * @param args Command line arguments that are put into main function.
   */
  public static <T> T parse(T toolArgument, String[] args) {
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

  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = NonEmptyStringField.class,
      required = true)
  public String brokers;

  /**
   * @param propsFile file path containing the properties to be passed to kafka
   * @return the kafka props consists of bootstrap servers and all props from file (if it is
   *     existent)
   */
  Map<String, Object> properties(String propsFile) {
    var props = new Properties();
    if (propsFile != null) {
      try (var input = new FileInputStream(propsFile)) {
        props.load(input);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
    return props.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
  }

  @Parameter(
      names = {"--prop.file"},
      description = "the file path containing the properties to be passed to kafka admin",
      validateWith = NonEmptyStringField.class)
  public String propFile;

  /**
   * @return the kafka props consists of bootstrap servers and all props from file (if it is
   *     existent)
   */
  public Map<String, Object> props() {
    return properties(propFile);
  }
}
