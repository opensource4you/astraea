package org.astraea.metrics.jmx;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;

/**
 * A MBeanClient used to retrieve mbean value from remote Jmx server.
 *
 * <pre>{@code
 * try(MBeanClient client = new MBeanClient(jmxConnectorServer.getAddress())) {
 *   BeanObject bean = client.queryBean(BeanQuery.builder("java.lang")
 *            .property("type", "MemoryManager")
 *            .property("name", "CodeCacheManager")
 *            .build());
 *   System.out.println(bean.getAttributes());
 * }</pre>
 */
public interface MBeanClient extends AutoCloseable {

  static MBeanClient of(JMXServiceURL url) {
    return new MBeanClientImpl(url);
  }

  /**
   * Fetch all attributes of target mbean.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @return A {@link BeanObject} contain all attributes if target resolved successfully.
   */
  BeanObject queryBean(BeanQuery beanQuery);

  /**
   * Fetch given attributes of target mbean
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @param attributeNameCollection a list of attribute you want to retrieve
   * @return A {@link BeanObject} contain given specific attributes if target resolved successfully.
   */
  BeanObject queryBean(BeanQuery beanQuery, Collection<String> attributeNameCollection);

  /**
   * Query mBeans by pattern.
   *
   * <p>Query mbeans by {@link ObjectName} pattern, the returned {@link BeanObject}s will contain
   * all the available attributes
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the pattern to query
   * @return A {@link Set} of {@link BeanObject}, all BeanObject has its own attributes resolved.
   */
  Collection<BeanObject> queryBeans(BeanQuery beanQuery);

  /**
   * Returns the list of domains in which any MBean is currently registered.
   *
   * <p>The order of strings within the returned array is not defined.
   *
   * @return a {@link List} of domain name {@link String}
   */
  List<String> listDomains();

  /** @return the host address of jmx server */
  String host();

  /** @return the port listened by jmx server */
  int port();

  @Override
  void close();
}
