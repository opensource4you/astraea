package org.astraea.performance;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ManagerTest {

  @Test
  void testGetKey() {
    var argument = new Performance.Argument();

    argument.keyDistributionType = DistributionType.UNIFORM;
    var manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);

    argument.keyDistributionType = DistributionType.ZIPFIAN;
    manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);

    argument.keyDistributionType = DistributionType.LATEST;
    manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);

    argument.keyDistributionType = DistributionType.FIXED;
    manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);
  }

  @Test
  void testConsumerDone() {
    var argument = new Performance.Argument();
    var producerMetrics = new Metrics();
    var consumerMetrics = new Metrics();

    argument.exeTime = ExeTime.of("1records");
    var manager = new Manager(argument, List.of(producerMetrics), List.of(consumerMetrics));

    // Produce one record
    producerMetrics.accept(0L, 0L);
    Assertions.assertFalse(manager.consumedDone());

    // Consume one record
    consumerMetrics.accept(0L, 0L);
    Assertions.assertTrue(manager.consumedDone());

    // Test zero consumer. (run for one record)
    manager = new Manager(argument, List.of(producerMetrics), List.of());
    Assertions.assertTrue(manager.consumedDone());
  }
}
