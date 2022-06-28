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
package org.astraea.app.admin;

import java.util.Collection;
import java.util.Objects;

public class Reassignment {
  private final Collection<Location> from;
  private final Collection<Location> to;

  Reassignment(Collection<Location> from, Collection<Location> to) {
    this.from = from;
    this.to = to;
  }

  /** @return the partition locations before migration */
  public Collection<Location> from() {
    return from;
  }

  /** @return the partition locations after migration */
  public Collection<Location> to() {
    return to;
  }

  @Override
  public String toString() {
    return "Reassignment{" + "from=" + from + ", to=" + to + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Reassignment that = (Reassignment) o;
    return Objects.equals(from, that.from) && Objects.equals(to, that.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }

  public static class Location {
    private final int broker;
    private final String path;

    public Location(int broker, String path) {
      this.broker = broker;
      this.path = Objects.requireNonNull(path);
    }

    public int broker() {
      return broker;
    }

    public String path() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Location location = (Location) o;
      return broker == location.broker && path.equals(location.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(broker, path);
    }

    @Override
    public String toString() {
      return "Location{" + "broker=" + broker + ", path='" + path + '\'' + '}';
    }
  }
}
