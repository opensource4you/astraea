package org.astraea.argument;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;

/** This basic argument defines the common property used by all kafka clients. */
public abstract class Argument {

  static String[] filterEmpty(String[] args) {
    return Arrays.stream(args)
        .map(String::trim)
        .filter(trim -> !trim.isEmpty())
        .toArray(String[]::new);
  }

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
      // filter the empty string
      jc.parse(filterEmpty(args));
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
  String bootstrapServers;

  /**
   * @return all configs from both "--configs" and "--prop.file". Other kafka-related configs are
   *     added also.
   */
  public Map<String, String> configs() {
    var all = new HashMap<>(configs);
    if (propFile != null) {
      var props = new Properties();
      try (var input = new FileInputStream(propFile)) {
        props.load(input);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      props.forEach((k, v) -> all.put(k.toString(), v.toString()));
    }
    all.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return Collections.unmodifiableMap(all);
  }

  public String bootstrapServers() {
    return bootstrapServers;
  }

  @Parameter(
      names = {"--prop.file"},
      description = "the file path containing the properties to be passed to kafka admin",
      validateWith = NonEmptyStringField.class)
  String propFile;

  @Parameter(
      names = {"--configs"},
      description = "Map: set configs by command-line. For example: --configs a=b,c=d",
      converter = StringMapField.class,
      validateWith = StringMapField.class)
  Map<String, String> configs = Map.of();
}
