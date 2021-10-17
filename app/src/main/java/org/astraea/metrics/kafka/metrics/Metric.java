package org.astraea.metrics.kafka.metrics;

import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;

/**
 * Abstraction to the operation of fetch/transform specific metric.
 *
 * <p>If a class implements this interface. It means that the class knows:
 *
 * <ol>
 *   <li>How to construct a query that used to fetch a specific MBean metric through {@link
 *       org.astraea.metrics.jmx.MBeanClient}.
 *   <li>When we got the return result {@link org.astraea.metrics.jmx.BeanObject}, I know how to
 *       wrap this object into something with concrete class implementation. Which is much easier to
 *       use than the raw object result.
 * </ol>
 *
 * This interface is here to de-couple the responsibility of, how to get specific metric and
 * transform it into a domain object, from any MBean client implementation like {@link
 * org.astraea.metrics.kafka.KafkaMetricClient}
 *
 * @param <RET_TYPE> The type for the class who know how to wrap {@link BeanObject} and provide easy
 *     API to use the {@link BeanObject}.
 */
public interface Metric<RET_TYPE> {

  /**
   * return a BeanQuery this Metric required to construct the domain object.
   *
   * @return a {@link BeanQuery} used to resolve specific MBean.
   */
  BeanQuery query();

  /**
   * construct the domain object with {@link RET_TYPE} type by the given {@link BeanObject}.
   *
   * @param beanObject a {@link BeanObject} resolved from {@link BeanQuery} of {@link #query()}.
   * @return a domain object based on given {@link BeanObject}, it offer easier API to use.
   */
  RET_TYPE from(BeanObject beanObject);
}
