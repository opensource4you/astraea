package org.astraea.performance;

import java.util.Arrays;
import java.util.List;
import org.astraea.utils.DataUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ManagerTest {
  @Test
  void testReturnNull() throws InterruptedException {
    var argument = new Performance.Argument();
    var producerMetric = new Metrics();
    argument.exeTime = ExeTime.of("1records");
    var manager = new Manager(argument, List.of(producerMetric), List.of());

    Assertions.assertTrue(manager.payload().isPresent());
    producerMetric.accept(1L, 1L);
    Assertions.assertTrue(manager.payload().isEmpty());

    producerMetric = new Metrics();
    argument.exeTime = ExeTime.of("50ms");
    manager = new Manager(argument, List.of(producerMetric), List.of());

    Assertions.assertTrue(manager.payload().isPresent());
    producerMetric.accept(1L, 1L);
    Thread.sleep(50);
    Assertions.assertTrue(manager.payload().isEmpty());
  }

  @Test
  void testRandomSize() {
    var argument = new Performance.Argument();
    argument.exeTime = ExeTime.of("3records");
    argument.recordSize = DataUnit.KiB.of(100);
    var dataManager = new Manager(argument, List.of(), List.of());
    boolean sameSize = dataManager.payload().get().length == dataManager.payload().get().length;

    // Assertion failed with probability 1/102400 ~ 0.001%
    Assertions.assertFalse(sameSize);

    Assertions.assertTrue(dataManager.payload().get().length <= 102400);
  }

  @Test
  void testRandomContent() {
    var argument = new Performance.Argument();
    argument.exeTime = ExeTime.of("2records");
    argument.recordSize = DataUnit.KiB.of(100);
    var manager = new Manager(argument, List.of(), List.of());
    boolean same = Arrays.equals(manager.payload().get(), manager.payload().get());

    // Assertion failed with probability < 1/102400
    Assertions.assertFalse(same);
  }

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
    var producerMetrics = List.of(new Metrics());
    var consumerMetrics = List.of(new Metrics());

    argument.exeTime = ExeTime.of("1records");
    var manager = new Manager(argument, producerMetrics, consumerMetrics);
    Assertions.assertFalse(manager.consumedDone());

    // Produce one record
    producerMetrics.get(0).accept(0L, 0L);
    Assertions.assertFalse(manager.consumedDone());

    // Consume one record
    consumerMetrics.get(0).accept(0L, 0L);
    Assertions.assertFalse(manager.consumedDone());

    manager.producerClosed();
    Assertions.assertTrue(manager.consumedDone());

    // Test zero consumer. (run for one record)
    producerMetrics = List.of(new Metrics());
    consumerMetrics = List.of();
    manager = new Manager(argument, producerMetrics, consumerMetrics);
    Assertions.assertFalse(manager.consumedDone());

    // Produce one record
    producerMetrics.get(0).accept(0L, 0L);
    Assertions.assertFalse(manager.consumedDone());

    manager.producerClosed();
    Assertions.assertTrue(manager.consumedDone());
  }

  @Test
  void testProducerDone() {
    var argument = new Performance.Argument();
    argument.exeTime = ExeTime.of("1records");
    var producerMetrics = List.of(new Metrics());
    var consumerMetrics = List.of(new Metrics());
    var manager = new Manager(new Performance.Argument(), producerMetrics, consumerMetrics);

    Assertions.assertFalse(manager.producedDone());

    producerMetrics.get(0).accept(0L, 0L);
    Assertions.assertFalse(manager.producedDone());

    manager.producerClosed();
    Assertions.assertTrue(manager.producedDone());
  }

  @Test
  void testCheckAndAdd() throws InterruptedException {
    var argument = new Performance.Argument();
    argument.throughput = DataUnit.KiB.of(3);
    var recordSize = DataUnit.KiB.of(1).measurement(DataUnit.Byte).intValue();
    var manager = new Manager(argument, List.of(), List.of());

    Assertions.assertTrue(manager.checkAndAdd(recordSize));
    Assertions.assertTrue(manager.checkAndAdd(recordSize));
    Assertions.assertTrue(manager.checkAndAdd(recordSize));
    Assertions.assertFalse(manager.checkAndAdd(recordSize));

    Thread.sleep(1001);

    Assertions.assertTrue(manager.checkAndAdd(recordSize));
  }
}
