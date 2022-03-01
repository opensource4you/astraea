package org.astraea.metrics.collector;

import java.util.List;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class FetcherTest {

  @Test
  void testMultipleFetchers() {
    var mbean0 = Mockito.mock(HasBeanObject.class);
    Fetcher fetcher0 = client -> List.of(mbean0);
    var mbean1 = Mockito.mock(HasBeanObject.class);
    Fetcher fetcher1 = client -> List.of(mbean1);

    var fetcher = Fetcher.of(List.of(fetcher1, fetcher0));

    var result = fetcher.fetch(Mockito.mock(MBeanClient.class));

    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.contains(mbean0));
    Assertions.assertTrue(result.contains(mbean1));
  }
}
