package org.astraea.yunikorn.config;

import java.math.BigInteger;
import java.util.Map;

public class ChildTemplate {

  private BigInteger maxapplications;

  private Map<String, String> properties;
  private Resources resourves;

  public BigInteger getMaxapplications() {
    return maxapplications;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Resources getResourves() {
    return resourves;
  }

  public void setMaxapplications(BigInteger maxapplications) {
    this.maxapplications = maxapplications;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setResourves(Resources resourves) {
    this.resourves = resourves;
  }
}
