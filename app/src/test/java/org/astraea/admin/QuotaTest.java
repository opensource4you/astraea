package org.astraea.admin;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QuotaTest {

  @Test
  void testTarget() {
    Arrays.stream(Quota.Target.values())
        .forEach(t -> Assertions.assertEquals(t, Quota.target(t.nameOfKafka())));
  }

  @Test
  void testAction() {
    Arrays.stream(Quota.Limit.values())
        .forEach(t -> Assertions.assertEquals(t, Quota.limit(t.nameOfKafka())));
  }
}
