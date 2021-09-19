package org.astraea.metrics.jmx;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.*;
import javax.management.remote.*;

/**
 * A MBeanClient used to retrieve mbean value from remote Jmx server.
 *
 * <pre>{@code
 * MBeanClient client = new MBeanClient(jmxConnectorServer.getAddress());
 * BeanObject bean = client.fetchAttributes(BeanQuery.of("java.lang")
 *          .whereProperty("type", "MemoryManager")
 *          .whereProperty("name", "CodeCacheManager"));
 * System.out.println(bean.getAttributes());
 * }</pre>
 */
public class MBeanClient implements AutoCloseable {

  private final JMXServiceURL jmxServiceURL;
  private final JMXConnector jmxConnector;
  private final MBeanServerConnection mBeanServerConnection;
  private final AtomicBoolean isClosed;

  public MBeanClient(JMXServiceURL jmxServiceURL) throws IOException {
    this.jmxServiceURL = Objects.requireNonNull(jmxServiceURL);
    this.jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
    jmxConnector.connect();
    this.mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    this.isClosed = new AtomicBoolean(false);
  }

  /**
   * Fetch all attributes of target mbean.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exactly
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @return A {@link BeanObject} contain all attributes if target resolved successfully.
   * @throws InstanceNotFoundException If the pattern target doesn't exists on remote mbean server.
   */
  public BeanObject fetchAttributes(BeanQuery beanQuery) throws InstanceNotFoundException {
    ensureConnected();
    try {
      // ask for MBeanInfo
      MBeanInfo mBeanInfo = mBeanServerConnection.getMBeanInfo(beanQuery.objectName());

      // create a list of all available attributes name
      String[] attributeName =
          Arrays.stream(mBeanInfo.getAttributes())
              .map(MBeanFeatureInfo::getName)
              .toArray(String[]::new);

      // query the result
      return fetchAttributes(beanQuery, attributeName);
    } catch (ReflectionException | IntrospectionException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetch given attributes of target mbean
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exactly
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @param attributeNameList a list of attribute you want to retrieve
   * @return A {@link BeanObject} contain given specific attributes if target resolved successfully.
   * @throws InstanceNotFoundException If the pattern target doesn't exists on remote mbean server.
   */
  public BeanObject fetchAttributes(BeanQuery beanQuery, String[] attributeNameList)
      throws InstanceNotFoundException {
    ensureConnected();
    try {

      // fetch attribute value from mbean server
      List<Attribute> attributeList =
          mBeanServerConnection.getAttributes(beanQuery.objectName(), attributeNameList).asList();

      // collect attribute name & value into a map
      Map<String, Object> attributes = new HashMap<>();
      for (Attribute attribute : attributeList) {
        attributes.put(attribute.getName(), attribute.getValue());
      }

      // according to the javadoc of MBeanServerConnection#getAttributes, the API will ignore any
      // error occurring during the fetch process (for example, attribute not exists). Below code
      // check for such condition and try to figure out what exactly the error is. put it into
      // attributes return result.
      Set<String> notResolvedAttributes =
          Arrays.stream(attributeNameList)
              .filter(str -> !attributes.containsKey(str))
              .collect(Collectors.toSet());
      for (String attributeName : notResolvedAttributes) {
        attributes.put(attributeName, fetchAttributeObjectOrException(beanQuery, attributeName));
      }

      // collect result, and build a ne BeanObject as return result
      return new BeanObject(beanQuery.domainName(), beanQuery.properties(), attributes);

    } catch (ReflectionException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object fetchAttributeObjectOrException(BeanQuery beanQuery, String attributeName) {
    // It is possible to trigger some unexpected runtime exception during the following call.
    // For example, on my machine when I try to get attribute "BootClassPath" of
    // "java.lang:type=Runtime".
    // I will get a {@link java.lang.UnsupportedOperationException} indicates that "Boot class path
    // mechanism is not supported". Those attribute actually exists, but I cannot retrieve those
    // attribute value. Doing so I get that error.
    //
    // Instead of blinding that attribute from the library user, I decided to put the exception
    // into their result.
    try {
      return mBeanServerConnection.getAttribute(beanQuery.objectName(), attributeName);
    } catch (Exception e) {
      return e;
    }
  }

  /**
   * Fetch all attributes of target mbean, return with {@link Optional} support
   *
   * <p>During the attribute retrieve process, if a {@link InstanceNotFoundException} is raised, an
   * {@link Optional#empty()} will return.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exactly
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @return A {@link Optional<BeanObject>} of {@link BeanObject} contain all attributes if target
   *     resolved successfully. If the pattern target doesn't exists on remote mbean server, then an
   *     {@link Optional#empty()} returned.
   */
  public Optional<BeanObject> tryFetchAttributes(BeanQuery beanQuery) {
    ensureConnected();
    try {
      return Optional.of(this.fetchAttributes(beanQuery));
    } catch (InstanceNotFoundException e) {
      return Optional.empty();
    }
  }

  /**
   * Fetch a list of mbean attribute, return with {@link Optional} support
   *
   * <p>During the attribute retrieve process, if a {@link InstanceNotFoundException} is raised, an
   * {@link Optional#empty()} will return.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exactly
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @param attributeNameList array of attribute names you want to retrieve
   * @return A {@link Optional<BeanObject>} contain the exactly {@link BeanObject} with specific
   *     attributes resolved if target mbeans resolved successfully. If the pattern target doesn't
   *     exists on remote mbean server, then an {@link Optional#empty()} returned.
   */
  public Optional<BeanObject> tryFetchAttributes(BeanQuery beanQuery, String[] attributeNameList) {
    ensureConnected();
    try {
      return Optional.of(this.fetchAttributes(beanQuery, attributeNameList));
    } catch (InstanceNotFoundException e) {
      return Optional.empty();
    }
  }

  /**
   * Query mBeans by pattern.
   *
   * <p>Query mbeans by {@link ObjectName} pattern, the returned {@link BeanObject}s will contain
   * all the available attributes
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exactly
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the pattern to query
   * @return A {@link Set} of {@link BeanObject}, all BeanObject has its own attributes resolved.
   */
  public Set<BeanObject> queryBeans(BeanQuery beanQuery) {
    ensureConnected();
    try {

      // query mbeans
      Set<ObjectInstance> objectInstances =
          mBeanServerConnection.queryMBeans(beanQuery.objectName(), null);

      // transform result into a set of BeanQuery
      Stream<BeanQuery> queries =
          objectInstances.stream().map(ObjectInstance::getObjectName).map(BeanQuery::of);

      // execute query on each BeanQuery, return result as a set of BeanObject
      Set<BeanObject> queryResult =
          queries
              .map(this::tryFetchAttributes)
              .flatMap(Optional::stream)
              .collect(Collectors.toSet());

      return queryResult;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void ensureConnected() {
    if (isClosed.get()) throw new IllegalStateException("MBean client is closed");
  }

  @Override
  public void close() throws Exception {
    this.isClosed.set(true);
    this.jmxConnector.close();
  }
}
