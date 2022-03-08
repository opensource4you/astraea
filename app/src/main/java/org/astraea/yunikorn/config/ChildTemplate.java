package org.astraea.yunikorn.config;

import java.math.BigInteger;
import java.util.Map;
import lombok.*;

@Getter @Setter
public class ChildTemplate {

      private BigInteger maxapplications;

      private Map<String, String> properties;
      private Resources resourves ;

}
