package org.astraea.service;

import java.lang.management.ManagementFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.Utils;
import org.junit.jupiter.api.AfterAll;

public abstract class RequireJmxServer {

  private static final JMXConnectorServer JMX_CONNECTOR_SERVER = jmxConnectorServer();

  protected static JMXServiceURL jmxServiceURL() {
    return JMX_CONNECTOR_SERVER.getAddress();
  }

  @AfterAll
  static void shutdownJmxServer() {
    Utils.close(JMX_CONNECTOR_SERVER::stop);
  }

  /**
   * create a mbean server for local JVM.
   *
   * @return JMXConnectorServer
   */
  private static JMXConnectorServer jmxConnectorServer() {
    try {
      var serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");
      var mBeanServer = ManagementFactory.getPlatformMBeanServer();
      var jmxServer =
          JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServer);
      jmxServer.start();
      return jmxServer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
