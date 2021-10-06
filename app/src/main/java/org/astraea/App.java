package org.astraea;

import com.beust.jcommander.ParameterException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.metrics.kafka.KafkaMetricClientApp;
import org.astraea.offset.OffsetExplorer;
import org.astraea.performance.latency.End2EndLatency;

public class App {
  private static final List<Class<?>> MAIN_CLASSES =
      Arrays.asList(End2EndLatency.class, OffsetExplorer.class, KafkaMetricClientApp.class);

  private static String toString(List<Class<?>> mains) {
    return mains.stream().map(Class::getName).collect(Collectors.joining(","));
  }

  private static String usage(List<Class<?>> mains) {
    return "Usage: [" + toString(mains) + "] [args ...]";
  }

  static void execute(List<Class<?>> mains, List<String> args) throws Throwable {
    if (args.size() < 1) {
      throw new IllegalArgumentException(usage(mains));
    }

    var className = args.get(0);

    var targetClass =
        mains.stream()
            .filter(clz -> clz.getName().contains(className))
            .findFirst()
            .orElseThrow(
                (Supplier<Throwable>)
                    () ->
                        new IllegalArgumentException(
                            "className: " + className + " is not matched to " + toString(mains)));

    var method = targetClass.getDeclaredMethod("main", String[].class);
    try {
      method.invoke(null, (Object) args.subList(1, args.size()).toArray(String[]::new));
    } catch (InvocationTargetException targetException) {
      // Print out ParameterException, don't throw.
      if (targetException.getTargetException() instanceof ParameterException) {
        System.out.println(targetException.getTargetException().getMessage());
      } else {
        throw targetException.getTargetException();
      }
    }
  }

  public static void main(String[] args) throws Throwable {
    execute(MAIN_CLASSES, Arrays.asList(args));
  }
}
