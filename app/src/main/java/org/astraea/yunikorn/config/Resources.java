package org.astraea.yunikorn.config;

import java.util.Map;

public class Resources {
  private Map<String, String> guaranteed;
  private Map<String, String> max;

  public void setGuaranteed(Map<String, String> guaranteed) {
    this.guaranteed = guaranteed;
  }

  public void setMax(Map<String, String> max) {
    this.max = max;
  }

  public Map<String, String> getGuaranteed() {
    return guaranteed;
  }

  public Map<String, String> getMax() {
    return max;
  }
}
