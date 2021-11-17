package org.astraea.performance;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataManagerTest {
  @Test
  void testReturnNull() {
    var dataManager = new DataManager(1, false, 1024);

    Assertions.assertTrue(dataManager.payload().isPresent());
    Assertions.assertTrue(dataManager.payload().isEmpty());
  }

  @Test
  void testRandomSize() {
    var dataManager = new DataManager(3, false, 102400);
    boolean sameSize = dataManager.payload().get().length == dataManager.payload().get().length;

    // Assertion failed with probability 1/102400 ~ 0.001%
    Assertions.assertFalse(sameSize);

    Assertions.assertTrue(dataManager.payload().get().length <= 102400);
  }

  @Test
  void testRandomContent() {
    var dataManager = new DataManager(2, false, 102400);
    boolean same = Arrays.equals(dataManager.payload().get(), dataManager.payload().get());

    // Assertion failed with probability < 1/102400
    Assertions.assertFalse(same);
  }
}
