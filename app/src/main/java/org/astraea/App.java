package org.astraea;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.performance.Performance;
import org.astraea.performance.latency.End2EndLatency;

public class App {
  private static final List<Class<?>> MAIN_CLASSES =
      Arrays.asList(End2EndLatency.class, Performance.class);

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
    method.invoke(null, (Object) args.subList(1, args.size()).toArray(String[]::new));
  }

  public static void main(String[] args) throws Throwable {
    execute(MAIN_CLASSES, Arrays.asList(args));
  }
}
