package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.net.MalformedURLException;
import org.junit.jupiter.api.Test;

class MBeanClientTest {

  @Test
  void fetchAttributeWithoutConnectFirstShouldThrowError() throws MalformedURLException {
    MBeanClient mockClient = new MBeanClient("service:jmx:rmi:///jndi/rmi://example:5566/jmxrmi");
    BeanObject mockObject = new BeanObject("");

    assertThrows(IllegalStateException.class, () -> mockClient.fetchObjectAttribute(mockObject));
  }
}
