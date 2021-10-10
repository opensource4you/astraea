package org.astraea.argument;

import com.beust.jcommander.Parameter;
import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;

/** This basic argument defines the common property used by all kafka clients. */
public abstract class BasicArgument {
  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = ArgumentUtil.NotEmptyString.class,
      required = true)
  public String brokers;

  @Parameter(
      names = {"--props.file"},
      description = "the properties file to load for kafka client",
      validateWith = ArgumentUtil.NotEmptyString.class)
  public String propsFile;

  /**
   * @return the kafka props consists of bootstrap servers and all props from file (if it is
   *     existent)
   */
  public Map<String, Object> properties() {
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
}
