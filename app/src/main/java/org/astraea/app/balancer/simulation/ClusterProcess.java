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
package org.astraea.app.balancer.simulation;

import java.util.Collection;
import org.astraea.app.balancer.simulation.events.ClusterEvent;
import org.astraea.app.balancer.simulation.events.TopicCreationEvent;

/**
 * A (maybe ongoing) random process of a specific cluster. Note that the implementation of this
 * process <strong>might not</strong> be independent of wall time.
 */
public interface ClusterProcess {

  /**
   * @return a collection of {@link TopicCreationEvent} created(sampled) over the specified
   *     observation time.
   */
  Collection<ClusterEvent> observe(long ms);
}
