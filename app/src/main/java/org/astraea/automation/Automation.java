package org.astraea.automation;

import static org.astraea.Utils.astraeaPath;
import static org.astraea.performance.Performance.performanceLatch;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.astraea.performance.Performance;

/**
 * By configuring the parameters in config/automation.properties, control the execution times of
 * performance and its configuration parameters.
 */
public class Automation {
  private static final List performanceProperties =
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
    ClassLoader classLoader = Performance.class.getClassLoader();
    try {
      var properties = new Properties();
      properties.load(new FileInputStream(astraeaPath() + "/config/automation.properties"));

      var i = 0;
      var times = 0;
      if (properties.getProperty("--time").equals("Defaults")) times = 5;
      else times = Integer.parseInt(properties.getProperty("--time"));

      while (i < times) {
        var loadClass = classLoader.loadClass("org.astraea.performance.Performance");
        var method = loadClass.getMethod("main", String[].class);
        method.invoke(null, (Object) performanceArgs(properties));
        performanceLatch().await();
        i++;
        System.out.println("=============== " + i + " time completed===============");
      }
    } catch (ClassNotFoundException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | IOException
        | InterruptedException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  private static String[] performanceArgs(Properties properties) {
    var args = new ArrayList<String>();
    performanceProperties.forEach(
        str -> {
          var property = properties.getProperty((String) str);
          if (property != null && !property.equals("Defaults")) {
            args.add((String) str);
            args.add(property);
          }
        });
    var strings = new String[args.size()];
    return args.toArray(strings);
  }
}
