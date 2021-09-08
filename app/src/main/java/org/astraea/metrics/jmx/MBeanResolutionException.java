package org.astraea.metrics.jmx;

/** This error is throwing when we failed to resolute a MBean from remote MBean server. */
public class MBeanResolutionException extends RuntimeException {
  public MBeanResolutionException(Exception e) {
    super(e);
  }
}
