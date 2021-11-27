package org.astraea.performance;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ManagerTest {
  @Test
  void testReturnNull() throws InterruptedException {
    var argument = new Performance.Argument();
    var producerMetric = new Metrics();
    argument.exeTime = new Performance.Argument.ExeTime(1);
    var manager = new Manager(argument, List.of(producerMetric), List.of());

    Assertions.assertTrue(manager.payload().isPresent());
    producerMetric.accept(1L, 1L);
    Assertions.assertTrue(manager.payload().isEmpty());

    producerMetric = new Metrics();
    argument.exeTime = new Performance.Argument.ExeTime(Duration.ofMillis(50));
    manager = new Manager(argument, List.of(producerMetric), List.of());

    Assertions.assertTrue(manager.payload().isPresent());
    producerMetric.accept(1L, 1L);
    Thread.sleep(50);
    Assertions.assertTrue(manager.payload().isEmpty());
  }

  @Test
  void testRandomSize() {
    var argument = new Performance.Argument();
    argument.exeTime = new Performance.Argument.ExeTime(3L);
    argument.recordSize = 102400;
    var dataManager = new Manager(argument, List.of(), List.of());
    boolean sameSize = dataManager.payload().get().length == dataManager.payload().get().length;

    // Assertion failed with probability 1/102400 ~ 0.001%
    Assertions.assertFalse(sameSize);

    Assertions.assertTrue(dataManager.payload().get().length <= 102400);
  }

  @Test
  void testRandomContent() {
    var argument = new Performance.Argument();
    argument.exeTime = new Performance.Argument.ExeTime(2L);
    argument.recordSize = 102400;
    var manager = new Manager(argument, List.of(), List.of());
    boolean same = Arrays.equals(manager.payload().get(), manager.payload().get());

    // Assertion failed with probability < 1/102400
    Assertions.assertFalse(same);
  }
}
