package org.astraea.partitioner.smoothPartitioner;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SmoothWeightTest {
  @Test
  public void testGetAndChoose() {
    var smoothWeight = new SmoothWeight(Map.of(1, 5.0, 2, 3.0, 3, 1.0));

    Assertions.assertEquals(1, smoothWeight.getAndChoose());
    Assertions.assertEquals(2, smoothWeight.getAndChoose());
    Assertions.assertEquals(1, smoothWeight.getAndChoose());
    Assertions.assertEquals(1, smoothWeight.getAndChoose());
    Assertions.assertEquals(2, smoothWeight.getAndChoose());
    Assertions.assertEquals(1, smoothWeight.getAndChoose());
    Assertions.assertEquals(3, smoothWeight.getAndChoose());
  }
}
