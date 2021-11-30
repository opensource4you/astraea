package org.astraea.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.Utils;
import org.junit.jupiter.api.AfterAll;

public abstract class RequireJmxServer {

  private static final JMXConnectorServer JMX_CONNECTOR_SERVER = jmxConnectorServer(freePort());

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
  public static JMXConnectorServer jmxConnectorServer(int port) {
    try {
      LocateRegistry.createRegistry(port);
      var mBeanServer = ManagementFactory.getPlatformMBeanServer();
      var jmxServer =
          JMXConnectorServerFactory.newJMXConnectorServer(
              // we usually use JNDI to get metrics in production, so the embedded server should use
              // JNDI too.
              new JMXServiceURL(
                  String.format(
                      "service:jmx:rmi://127.0.0.1:%s/jndi/rmi://127.0.0.1:%s/jmxrmi", port, port)),
              null,
              mBeanServer);
      jmxServer.start();
      return jmxServer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static int freePort() {
    try (var server = new ServerSocket(0)) {
      return server.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
