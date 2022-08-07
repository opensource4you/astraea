/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.astraea.app.argument.Argument;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
    Pattern pattern = MetricExplorer.Argument.JmxServerUrlField.patternOfJmxUrlStart;

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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "--jmx.server localhost:5566 --domain example.com",
        "--jmx.server localhost:5566 --domain example.com --view-object-name-list",
      })
  void executeDoesPrintSomething(String args) {
    // arrange
    var argument = Argument.parse(new MetricExplorer.Argument(), args.split(" "));
    var mockMBeanClient = mock(MBeanClient.class);
    when(mockMBeanClient.queryBeans(any()))
        .thenReturn(List.of(new BeanObject("example.com", Map.of("key", "value"), Map.of())));
    var stdout = System.out;
    var mockOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(mockOutput));

    // act
    Executable executable = () -> MetricExplorer.execute(mockMBeanClient, argument);

    // assert
    assertDoesNotThrow(executable);
    assertTrue(mockOutput.toString().contains("example.com"));
    assertTrue(mockOutput.toString().contains("key"));
    assertTrue(mockOutput.toString().contains("value"));

    // restore
    System.setOut(stdout);
  }

  @ParameterizedTest
  @CsvSource(
      delimiterString = "(is",
      value = {
        "--jmx.server localhost:5566                                       (is ok",
        "--jmx.server localhost:5566 --domain kafka.log                    (is ok",
        "--jmx.server localhost:5566 --property type=Memory                (is ok",
        "--jmx.server localhost:5566 --strict-match --property type=Memory (is ok",
        "--jmx.server localhost:5566 --view-object-name-list               (is ok",
        "wuewuewuewue                                                      (is not ok",
        "--view-object-name-list                                           (is not ok",
      })
  void ensureArgumentWorking(String argumentString, String outcome) {
    // arrange
    String[] arguments = argumentString.split(" ");

    // act
    Executable doParsing = () -> Argument.parse(new MetricExplorer.Argument(), arguments);

    // assert
    if (outcome.equals("ok")) {
      assertDoesNotThrow(doParsing);
    } else if (outcome.equals("not ok")) {
      assertThrows(Exception.class, doParsing);
    }
  }
}
