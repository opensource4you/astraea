package org.astraea.yunikorn.config;

import java.math.BigInteger;
import java.util.Map;


public class ChildTemplate {
      private BigInteger maxapplications;
      private Map<String, String> properties;
      private Resources resourves ;
      public Map<String, String> getProperties(){
            return this.properties;
      }
      public Resources getResourves(){
            return this.resourves;
      }

      public BigInteger getMaxapplications() {
            return maxapplications;
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
