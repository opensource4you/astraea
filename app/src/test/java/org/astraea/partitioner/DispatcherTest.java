package org.astraea.partitioner;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.cost.ClusterInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DispatcherTest {

  @Test
  void testNullKey() {
    var count = new AtomicInteger();
    var dispatcher =
        new Dispatcher() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            Assertions.assertEquals(0, Objects.requireNonNull(key).length);
            Assertions.assertEquals(0, Objects.requireNonNull(value).length);
            count.incrementAndGet();
            return 0;
          }

          @Override
          public void configure(Configuration config) {
            count.incrementAndGet();
          }
        };
    Assertions.assertEquals(0, count.get());
    // it should not throw NPE
    dispatcher.partition("t", null, null, null, null, null);
    Assertions.assertEquals(1, count.get());
    dispatcher.configure(Map.of("a", "b"));
    Assertions.assertEquals(2, count.get());
  }
}
