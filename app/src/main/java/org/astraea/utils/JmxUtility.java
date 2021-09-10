package org.astraea.utils;

public final class JmxUtility {

  private JmxUtility() {}

  public static String JMX_URI_FORMAT =
      "service:jmx:rmi:///jndi/rmi://" + "%s" + ":" + "%d" + "/jmxrmi";

  public static String createJmxUrl(String hostname, String port) {
    return String.format(JMX_URI_FORMAT, hostname, Integer.parseInt(port));
  }

  public static String createJmxUrl(String hostname, int port) {
    return String.format(JMX_URI_FORMAT, hostname, port);
  }
}
