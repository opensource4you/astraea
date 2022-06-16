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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.argument.Field;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.jmx.BeanQuery;
import org.astraea.app.metrics.jmx.MBeanClient;

public class MetricExplorer {

  static void execute(MBeanClient mBeanClient, Argument args) {
    // compile query
    BeanQuery.BeanQueryBuilder builder =
        BeanQuery.builder(
            args.fromDomainName,
            args.properties.stream()
                .map(Argument.CorrectPropertyFormat::toEntry)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    if (!args.strictMatch) builder.usePropertyListPattern();

    // execute query
    Collection<BeanObject> beanObjects = mBeanClient.queryBeans(builder.build());

    // display result
    if (args.viewObjectNameList) displayObjectNameList(beanObjects);
    else displayBeanObjects(beanObjects);
  }

  private static void displayBeanObjects(Collection<BeanObject> beanObjects) {
    var propertyOrderMap = propertyOrder(beanObjects);
    var collect = sortBeanObjects(beanObjects, propertyOrderMap);

    for (BeanObject beanObject : collect) {
      String properties =
          beanObject.getProperties().entrySet().stream()
              .sorted(Comparator.comparing(x -> propertyOrderMap.get(x.getKey())))
              .map(entry -> entry.getKey() + "=" + entry.getValue())
              .collect(Collectors.joining(","));

      System.out.printf("[%s:%s]\n", beanObject.domainName(), properties);

      Stream<String> strings = DataUtils.elaborateData(beanObject.getAttributes());
      strings = DataUtils.streamAppendWith(" ", 4, strings);
      strings.forEach(System.out::println);
    }
  }

  private static void displayObjectNameList(Collection<BeanObject> beanObjects) {
    var propertyOrderMap = propertyOrder(beanObjects);
    var collect = sortBeanObjects(beanObjects, propertyOrderMap);

    collect.forEach(
        x ->
            System.out.printf(
                "%s:%s\n",
                x.domainName(),
                x.getProperties().entrySet().stream()
                    .sorted(Comparator.comparingLong(z -> propertyOrderMap.get(z.getKey())))
                    .map(z -> z.getKey() + "=" + z.getValue())
                    .collect(Collectors.joining(","))));
  }

  /**
   * generate the order of property entry, the lower number the higher priority
   *
   * @param beanObjects Mbeans objects, the order is decided by some property or statistics based on
   *     the given Mbeans.
   * @return a {@link Map} describe the priority of each property key
   */
  static Map<String, Long> propertyOrder(Collection<BeanObject> beanObjects) {

    // count the frequency of each property key name
    // property who has higher frequency will close to the tree root when printing result
    Map<String, Long> propertyFrequencyMap =
        beanObjects.stream()
            .flatMap(x -> x.getProperties().entrySet().stream())
            .map(Map.Entry::getKey)
            .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

    // the order for sorting properties
    Map<String, Long> propertyOrderMap =
        propertyFrequencyMap.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), beanObjects.size() + 1 - entry.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // the property key "name" will always has the lowest priority, just like how JMC does it.
    propertyOrderMap.put("name", beanObjects.size() + 1L);

    return propertyOrderMap;
  }

  /**
   * sort the given bean objects based on the given property order. The order is first decide by the
   * alphabet order of domain name, then decided by the given property key order.
   *
   * @param beanObjects the Mbeans to be sorted.
   * @param propertyOrderMap the order of property key, the lower number the higher priority.
   * @return a new {@link List} consist of all the given Mbeans, all Mbeans are sort by given
   *     property order.
   */
  static List<BeanObject> sortBeanObjects(
      Collection<BeanObject> beanObjects, Map<String, Long> propertyOrderMap) {
    return beanObjects.stream()
        .sorted(
            Comparator.comparing(BeanObject::domainName)
                .thenComparing(
                    (o1, o2) -> {
                      List<Map.Entry<String, String>> order1 =
                          o1.getProperties().entrySet().stream()
                              .sorted(Comparator.comparing(x -> propertyOrderMap.get(x.getKey())))
                              .collect(Collectors.toList());
                      List<Map.Entry<String, String>> order2 =
                          o2.getProperties().entrySet().stream()
                              .sorted(Comparator.comparing(x -> propertyOrderMap.get(x.getKey())))
                              .collect(Collectors.toList());

                      // how the comparison works:
                      // 1. sort all properties, now property key/value with higher key frequency
                      // will show first.
                      // 2. sort by property key/value amount
                      // 3. compare the 1'st property key
                      // 4. compare the 2'nd property key
                      // 5. etc...
                      // 6. compare the 1'st property value
                      // 7. compare the 2'nd property value
                      // 8. etc...
                      // 9. object name equal

                      if (order1.size() != order2.size()) {
                        return -Integer.compare(order1.size(), order2.size());
                      } else {
                        var key =
                            Comparator.comparing(
                                (Map.Entry<String, String> x) -> propertyOrderMap.get(x.getKey()));
                        var value = Map.Entry.<String, String>comparingByValue();

                        for (int i = 0; i < order1.size(); i++) {
                          int result = key.compare(order1.get(i), order2.get(i));
                          if (result != 0) return result;
                        }

                        for (int i = 0; i < order1.size(); i++) {
                          int result = value.compare(order1.get(i), order2.get(i));
                          if (result != 0) return result;
                        }

                        return 0;
                      }
                    }))
        .collect(Collectors.toList());
  }

  public static void main(String[] args) {
    var arguments = org.astraea.app.argument.Argument.parse(new Argument(), args);

    try (var mBeanClient = MBeanClient.of(arguments.jmxServer)) {
      execute(mBeanClient, arguments);
    }
  }

  static class Argument {
    @Parameter(
        names = {"--jmx.server"},
        description =
            "The JMX server address to connect to, support [hostname:port] style or JMX URI format",
        validateWith = JmxServerUrlField.class,
        converter = JmxServerUrlField.class,
        required = true)
    JMXServiceURL jmxServer;

    @Parameter(
        names = {"--domain"},
        description =
            "Show Mbeans from specific domain pattern, support wildcard(*) and any char(?)")
    String fromDomainName = "*";

    @Parameter(
        names = {"--property"},
        description = "Fetch mbeans with specific property, support wildcard(*) and any char(?)",
        validateWith = CorrectPropertyFormat.class)
    List<String> properties = List.of();

    @Parameter(
        names = {"--strict-match"},
        description =
            "Only MBeans metrics with properties completely match the given criteria shows")
    boolean strictMatch = false;

    @Parameter(
        names = {"--view-object-name-list"},
        description = "Show the list view of MBeans' domain name & properties")
    boolean viewObjectNameList = false;

    public static class JmxServerUrlField extends Field<JMXServiceURL> {

      /** This regex used to test if a string look like a JMX URL */
      static Pattern patternOfJmxUrlStart = Pattern.compile("^service:jmx:rmi://");

      /**
       * Convert the given string to a {@link JMXServiceURL}.
       *
       * <p>Following string formats are supported
       *
       * <ol>
       *   <li>From hostname and port {@code "kafka1.example.com:9875"}
       *   <li>From JMX Service URL string {@code "service:jmx:rmi:///kafka1.example.come ..."}
       * </ol>
       */
      @Override
      public JMXServiceURL convert(String value) {
        Matcher matchJmxUrlStart = patternOfJmxUrlStart.matcher(value);

        if (matchJmxUrlStart.find()) {
          return convertFromJmxUrl(value);
        } else {
          return convertFromAddress(value);
        }
      }

      private JMXServiceURL convertFromJmxUrl(String jmxUrlString) {
        try {
          return new JMXServiceURL(jmxUrlString);
        } catch (MalformedURLException e) {
          throw new IllegalArgumentException("Cannot parse given URL: " + jmxUrlString, e);
        }
      }

      private JMXServiceURL convertFromAddress(String addressOfJmxServer) {
        String url = String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", addressOfJmxServer);
        try {
          return new JMXServiceURL(url);
        } catch (MalformedURLException e) {
          throw new IllegalArgumentException("Illegal JMX Url: " + url, e);
        }
      }
    }

    public static class CorrectPropertyFormat implements IParameterValidator {

      static Pattern propertyPattern = Pattern.compile("^(?<key>[^=]+)=(?<value>.+)$");

      @Override
      public void validate(String name, String value) throws ParameterException {
        if (!propertyPattern.matcher(value).find())
          throw new ParameterException(
              name
                  + "with value ["
                  + value
                  + "] should match format: "
                  + propertyPattern.pattern());
      }

      static Map.Entry<String, String> toEntry(String property) {
        Matcher matcher = propertyPattern.matcher(property);
        if (!matcher.find()) throw new IllegalArgumentException("Not property format: " + property);
        return Map.entry(matcher.group("key"), matcher.group("value"));
      }
    }
  }

  static class DataUtils {

    static int stringLengthLimit = 500;

    static Stream<String> elaborateData(Object target) {
      try {
        if (target == null) {
          return Stream.of("null");
        } else if (target instanceof List<?>) {
          return elaborateList((List<?>) target);
        } else if (target instanceof Map<?, ?>) {
          return elaborateMap((Map<?, ?>) target);
        } else if (target instanceof CompositeDataSupport) {
          return elaborateCompositeDataSupport(((CompositeDataSupport) target));
        } else if (target.getClass().isArray()) {
          if (target.getClass() == long[].class) return elaborateArray((long[]) target);
          else return elaborateArray((Object[]) target);
        }

        // I have no idea how to display this object, just toString it.
        return Stream.of(stringify(target));
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static String stringify(Object object) {
      String string = object.toString();
      if (string.length() > stringLengthLimit) {
        return string.substring(0, stringLengthLimit)
            + String.format("%n... (%d characters truncated)", string.length() - stringLengthLimit);
      } else {
        return string;
      }
    }

    static Stream<String> elaborateMap(Map<?, ?> data) {
      try {
        return data.entrySet().stream()
            .sorted(Comparator.comparing(Object::toString))
            .flatMap(DataUtils::elaborateMapEntry);
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static Stream<String> elaborateMapEntry(Map.Entry<?, ?> entry) {
      try {
        List<String> key = elaborateData(entry.getKey()).collect(Collectors.toList());
        List<String> value = elaborateData(entry.getValue()).collect(Collectors.toList());

        if (key.size() == 1 && value.size() > 1)
          return Stream.concat(
              Stream.of(String.format("\"%s\":", key.get(0))),
              streamAppendWith(" ", 4, value.stream()));

        return Stream.of(String.format("\"%s\": %s", key.get(0), value.get(0)));
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static Stream<String> elaborateArray(Object[] array) {
      try {
        return IntStream.range(0, array.length)
            .boxed()
            .flatMap(
                i -> {
                  List<String> collect = elaborateData(array[i]).collect(Collectors.toList());
                  if (collect.size() == 1)
                    return Stream.of(String.format("%d: %s", i, collect.get(0)));
                  else
                    return Stream.concat(
                        Stream.of(i + ":"), streamAppendWith(" ", 4, collect.stream()));
                });
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static Stream<String> elaborateArray(long[] array) {
      try {
        return IntStream.range(0, array.length).mapToObj(x -> x + ": " + array[x]);
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static Stream<String> elaborateList(List<?> list) {
      try {
        return IntStream.range(0, list.size())
            .boxed()
            .flatMap(
                i -> {
                  List<String> collect = elaborateData(list.get(i)).collect(Collectors.toList());
                  if (collect.size() == 1)
                    return Stream.of(String.format("%d: %s", i, collect.get(0)));
                  else
                    return Stream.concat(
                        Stream.of(i + ":"), streamAppendWith(" ", 4, collect.stream()));
                });
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static Stream<String> elaborateCompositeDataSupport(CompositeDataSupport data) {
      try {
        return data.getCompositeType().keySet().stream()
            .sorted()
            .flatMap(key -> DataUtils.elaborateMapEntry(Map.entry(key, data.get(key))));
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }

    static Stream<String> streamAppendWith(String s, int size, Stream<String> source) {
      try {
        String append = String.join("", Collections.nCopies(size, s));
        return source.map(x -> append + x);
      } catch (Exception e) {
        return Stream.of(e.toString());
      }
    }
  }
}
