package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.net.MalformedURLException;
import org.junit.jupiter.api.Test;

class MBeanClientTest {

  @Test
  void fetchAttributeWithoutConnectFirstShouldThrowError() throws MalformedURLException {
    final MBeanClient mBeanClient =
        new MBeanClient("service:jmx:rmi:///jndi/rmi://example:5566/jmxrmi");
    final BeanObject beanObject = new BeanObject("");
    final BeanQuery beanQuery = new BeanQuery("");

    assertThrows(IllegalStateException.class, () -> mBeanClient.fetchObjectAttribute(beanObject));
    assertThrows(IllegalStateException.class, () -> mBeanClient.queryObject(beanQuery));
  }
}
