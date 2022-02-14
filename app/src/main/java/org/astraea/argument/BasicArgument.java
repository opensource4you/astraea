package org.astraea.argument;

import com.beust.jcommander.Parameter;
import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.astraea.argument.validator.NotEmptyString;

/** This basic argument defines the common property used by all kafka clients. */
public abstract class BasicArgument {
  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = NotEmptyString.class,
      required = true)
  public String brokers;

  /**
   * @param propsFile file path containing the properties to be passed to kafka
   * @return the kafka props consists of bootstrap servers and all props from file (if it is
   *     existent)
   */
  protected Map<String, Object> properties(String propsFile) {
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
