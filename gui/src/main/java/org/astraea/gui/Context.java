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
package org.astraea.gui;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.astraea.common.admin.Admin;

public class Context {
  private final AtomicReference<Admin> atomicReference = new AtomicReference<>();

  public Optional<Admin> replace(Admin admin) {
    return Optional.ofNullable(atomicReference.getAndSet(admin));
  }

  public <T> T submit(Function<Admin, T> executor) {
    var admin = atomicReference.get();
    if (admin == null) throw new IllegalArgumentException("Please define bootstrap servers");
    return executor.apply(admin);
  }

  public void execute(Consumer<Admin> executor) {
    var admin = atomicReference.get();
    if (admin == null) throw new IllegalArgumentException("Please define bootstrap servers");
    executor.accept(admin);
  }
}
