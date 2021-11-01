package org.astraea.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MetricExplorerTest {

  @ParameterizedTest
  @CsvSource(
      delimiterString = "->",
      value = {
        "service:jmx:rmi://localhost:1234/jndi/rmi://localhost:2344/jmxrmi     -> match",
        "service:jmx:rmi:///jndi/rmi://:9999/jmxrmi                            -> match",
        "service:jmx:rmi://myhost/jndi/rmi://myhost:1099/myhost/myjmxconnector -> match",
        "service:jmx:rmi:///jndi/rmi://localhost:9987/jmxrmi                   -> match",
        "aaaaaaaaaaaaaaaaaaa                                                   -> no-match",
        "bbbbbbbbbbbbbbbbbbb                                                   -> no-match",
      })
  void testPatternJmxUrlStart(String url, String expected) {
    // arrange
    Pattern pattern = MetricExplorer.Argument.JmxServerUrlConverter.patternOfJmxUrlStart;

    // act
    Matcher matcher = pattern.matcher(url);

    // assert
    assertEquals(expected, matcher.find() ? "match" : "no-match");
  }

  @ParameterizedTest
  @CsvSource(
      delimiterString = "->",
      value = {
        "type=Memory                      -> type            -> Memory",
        "type=OperatingSystem             -> type            -> OperatingSystem",
        "key=value                        -> key             -> value",
        "logDirectory=\"/tmp/kafka-logs\" -> logDirectory    -> \"/tmp/kafka-logs\"",
        "face=\"= =\"                     -> face            -> \"= =\"",
      })
  void testPatternProperty(String property, String key, String value) {
    // arrange
    Pattern pattern = MetricExplorer.Argument.CorrectPropertyFormat.propertyPattern;

    // act
    Matcher matcher = pattern.matcher(property);

    // assert
    assertTrue(matcher.find());
    assertEquals(key, matcher.group("key"));
    assertEquals(value, matcher.group("value"));
  }
}
