package org.astraea.metrics;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.argument.ArgumentUtil;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;

public class MetricExplorer {

  static void execute(MBeanClient mBeanClient, Argument args) {

    BeanQuery.BeanQueryBuilder builder =
        BeanQuery.builder(
            args.fromDomainName,
            args.properties.stream()
                .map(Argument.CorrectPropertyFormat::toEntry)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    if(!args.strictMatch)
      builder.usePropertyListPattern();

    Collection<BeanObject> beanObjects = mBeanClient.queryBeans(builder.build());

    for (BeanObject beanObject : beanObjects) {
      System.out.println(beanObject);
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
    List<String> properties;

    @Parameter(
        names = {"--strict-match"},
        description =
            "Only MBeans metrics with properties completely match the given criteria shows")
    boolean strictMatch = false;

    static class JmxServerUrlConverter implements IStringConverter<JMXServiceURL> {

      static Pattern patternOfJmxUrlStart = Pattern.compile("^service:jmx:rmi");
      static Pattern patternOfHostnameIp =
          Pattern.compile("^(?<hostname>[[^\\p{Punct}]&&[.]]+):(?<ip>[0-9]{1,6})$");

      @Override
      public JMXServiceURL convert(String value) {

        if (patternOfJmxUrlStart.matcher(value).find()) {
          try {
            return new JMXServiceURL(value);
          } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Cannot parse given URL: " + value, e);
          }
        } else {
          Matcher matcher = patternOfHostnameIp.matcher(value);
          if (matcher.find()) {
            String hostname = matcher.group("hostname");
            String ip = matcher.group("ip");
            String url =
                String.format(
                    "service:jmx:rmi:///jndi/rmi://" + "%s" + ":" + "%s" + "/jmxrmi", hostname, ip);
            try {
              return new JMXServiceURL(url);
            } catch (MalformedURLException e) {
              throw new IllegalArgumentException("Illegal JMX url: " + url, e);
            }
          } else {
            throw new IllegalArgumentException("Illegal hostname:ip pair: " + value);
          }
        }
      }
    }

    static class CorrectPropertyFormat implements IParameterValidator {

      static Pattern propertyPattern = Pattern.compile("^(?<key>.+)=(?<value>.+)$");

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
}
