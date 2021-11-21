package org.astraea.performance;

import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ManagerTest {
  @Test
  void testReturnNull() throws InterruptedException {
    var manager = new Manager(1, Duration.ofSeconds(1), false, 1024, 1);
    Assertions.assertTrue(manager.payload().isPresent());
    Assertions.assertTrue(manager.payload().isEmpty());

    manager = new Manager(2, Duration.ofMillis(50), false, 1024, 1);
    Assertions.assertTrue(manager.payload().isPresent());
    Thread.sleep(50);
    Assertions.assertTrue(manager.payload().isEmpty());
  }

  @Test
  void testRandomSize() {
    var dataManager = new Manager(3, Duration.ofSeconds(1), false, 102400, 1);
    boolean sameSize = dataManager.payload().get().length == dataManager.payload().get().length;

    // Assertion failed with probability 1/102400 ~ 0.001%
    Assertions.assertFalse(sameSize);

    Assertions.assertTrue(dataManager.payload().get().length <= 102400);
  }

  @Test
  void testRandomContent() {
    var dataManager = new Manager(2, Duration.ofSeconds(1), false, 102400, 1);
    boolean same = Arrays.equals(dataManager.payload().get(), dataManager.payload().get());

    // Assertion failed with probability < 1/102400
    Assertions.assertFalse(same);
  }
}
