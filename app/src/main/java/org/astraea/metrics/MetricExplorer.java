package org.astraea.metrics;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXServiceURL;
import org.astraea.argument.ArgumentUtil;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;

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
    display(beanObjects);
  }

  static void display(Collection<BeanObject> beanObjects) {
    for (BeanObject beanObject : beanObjects) {
      String properties =
          beanObject.getProperties().entrySet().stream()
              .map(entry -> entry.getKey() + "=" + entry.getValue())
              .sorted()
              .collect(Collectors.joining(","));

      System.out.printf("[%s:%s]\n", beanObject.domainName(), properties);

      Stream<String> strings = DataUtils.elaborateData(beanObject.getAttributes());
      strings = DataUtils.streamAppendWith(" ", 4, strings);
      strings.forEach(System.out::println);
    }
  }

  public static void main(String[] args) throws Exception {
    var arguments = ArgumentUtil.parseArgument(new Argument(), args);

    try (MBeanClient mBeanClient = new MBeanClient(arguments.jmxServer)) {
      execute(mBeanClient, arguments);
    }
  }

  static class Argument {
    @Parameter(
        names = {"--jmx.server"},
        description =
            "The JMX server address to connect to, support [hostname:port] style or JMX URI format, former style assume no security and no password used",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = Argument.JmxServerUrlConverter.class,
        required = true)
    JMXServiceURL jmxServer;

    @Parameter(
        names = {"--from-domain"},
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

    public static class JmxServerUrlConverter implements IStringConverter<JMXServiceURL> {

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

    static Stream<String> elaborateData(Object target) {
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
      return Stream.of(target.toString());
    }

    static Stream<String> elaborateMap(Map<?, ?> data) {
      return data.entrySet().stream()
          .sorted(Comparator.comparing(Object::toString))
          .flatMap(DataUtils::elaborateMapEntry)
          .limit(50);
    }

    static Stream<String> elaborateMapEntry(Map.Entry<?, ?> entry) {
      List<String> key = elaborateData(entry.getKey()).collect(Collectors.toList());
      List<String> value = elaborateData(entry.getValue()).collect(Collectors.toList());

      if (key.size() == 1 && value.size() > 1)
        return Stream.concat(
            Stream.of(String.format("\"%s\":", key.get(0))),
            streamAppendWith(" ", 4, value.stream()));

      return Stream.of(String.format("\"%s\": %s", key.get(0), value.get(0)));
    }

    static Stream<String> elaborateArray(Object[] array) {
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
    }

    static Stream<String> elaborateArray(long[] array) {
      return IntStream.range(0, array.length).mapToObj(x -> x + ": " + array[x]).limit(20);
    }

    static Stream<String> elaborateList(List<?> list) {
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
    }

    static Stream<String> elaborateCompositeDataSupport(CompositeDataSupport data) {
      return data.getCompositeType().keySet().stream()
          .sorted()
          .flatMap(key -> DataUtils.elaborateMapEntry(Map.entry(key, data.get(key))))
          .limit(50);
    }

    static Stream<String> streamAppendWith(String s, int size, Stream<String> source) {
      String append = String.join("", Collections.nCopies(size, s));
      return source.map(x -> append + x);
    }
  }
}
