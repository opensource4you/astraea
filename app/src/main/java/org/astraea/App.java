package org.astraea;

import com.beust.jcommander.ParameterException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.astraea.metrics.MetricExplorer;
import org.astraea.performance.Performance;
import org.astraea.topic.ReplicaCollie;
import org.astraea.topic.TopicExplorer;
import org.astraea.topic.cost.PartitionScore;

public class App {
  private static final Map<String, Class<?>> MAIN_CLASSES =
      Map.of(
          "offset", TopicExplorer.class,
          "metrics", MetricExplorer.class,
          "replica", ReplicaCollie.class,
          "score", PartitionScore.class,
          "performance", Performance.class);

  static void execute(Map<String, Class<?>> mains, List<String> args) throws Throwable {

    var usage = "Usage: " + mains.keySet() + " [args ...]";

    if (args.size() < 1) {
      throw new IllegalArgumentException(usage);
    }

    var className = args.get(0);

    var targetClass = mains.get(className);
    if (targetClass == null) throw new IllegalArgumentException(usage);

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
