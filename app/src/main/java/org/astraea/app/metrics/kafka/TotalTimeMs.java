/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.metrics.kafka;

import java.util.Map;
import org.astraea.app.metrics.jmx.BeanObject;

public class TotalTimeMs implements HasPercentiles, HasCount, HasStatistics {

  private final BeanObject beanObject;

  public TotalTimeMs(BeanObject beanObject) {
    this.beanObject = beanObject;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> e : beanObject().getAttributes().entrySet()) {
      sb.append(System.lineSeparator())
          .append("  ")
          .append(e.getKey())
          .append("=")
          .append(e.getValue());
    }
    return beanObject.getProperties().get("request") + " TotalTimeMs {" + sb + "}";
  }

  @Override
  public BeanObject beanObject() {
    return beanObject;
  }
}
