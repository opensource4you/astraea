package org.astraea.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
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
  private static JMXConnectorServer jmxConnectorServer(int port) {
    try {
      LocateRegistry.createRegistry(port);
      var mBeanServer = ManagementFactory.getPlatformMBeanServer();
      var jmxServer =
          JMXConnectorServerFactory.newJMXConnectorServer(
              // we usually use JNDI to get metrics in production, so the embedded server should use
              // JNDI too.
              new JMXServiceURL(
                  String.format(
                      "service:jmx:rmi://%s:%s/jndi/rmi://%s:%s/jmxrmi",
                      address(), port, address(), port)),
              null,
              mBeanServer);
      jmxServer.start();
      return jmxServer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static int freePort() {
    try (var server = new ServerSocket(0)) {
      return server.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String address() {
    try {
      var address = InetAddress.getLocalHost().getHostAddress();
      if (address.equals("0.0.0.0") || address.equals("127.0.0.1"))
        throw new RuntimeException("the address of host can't be either 0.0.0.0 or 127.0.0.1");
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
