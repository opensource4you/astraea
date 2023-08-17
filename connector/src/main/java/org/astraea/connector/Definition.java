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
package org.astraea.connector;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public record Definition(
    String name,
    Optional<Object> defaultValue,
    String documentation,
    Type type,
    BiConsumer<String, Object> validator) {

  @Override
  public Optional<Object> defaultValue() {
    // ConfigDef.NO_DEFAULT_VALUE is a placeholder used to represent the lack of a default value.
    return defaultValue.filter(v -> v != ConfigDef.NO_DEFAULT_VALUE);
  }

  /**
   * @return true if the configuration is required, and it has no default value.
   */
  public boolean required() {
    return defaultValue.filter(v -> v == ConfigDef.NO_DEFAULT_VALUE).isPresent();
  }

  public static Builder builder() {
    return new Builder();
  }

  static ConfigDef toConfigDef(Collection<Definition> defs) {
    var def = new ConfigDef();
    defs.forEach(
        d ->
            def.define(
                d.name(),
                ConfigDef.Type.valueOf(d.type().name()),
                d.required() ? ConfigDef.NO_DEFAULT_VALUE : d.defaultValue().orElse(null),
                (n, o) -> {
                  try {
                    d.validator().accept(n, o);
                  } catch (Exception e) {
                    throw new ConfigException(n, o, e.getMessage());
                  }
                },
                ConfigDef.Importance.MEDIUM,
                d.documentation()));
    return def;
  }

  public enum Type {
    BOOLEAN,
    STRING,
    INT,
    SHORT,
    LONG,
    DOUBLE,
    LIST,
    CLASS,
    PASSWORD
  }

  public static class Builder {
    private String name;
    private Object defaultValue;

    private String documentation = "";
    private Type type = Type.STRING;

    private BiConsumer<String, Object> validator = (l, h) -> {};

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder required() {
      return defaultValue(ConfigDef.NO_DEFAULT_VALUE);
    }

    public Builder documentation(String documentation) {
      this.documentation = documentation;
      return this;
    }

    public Builder defaultValue(Object defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder type(Type type) {
      this.type = type;
      return this;
    }

    public Builder validator(BiConsumer<String, Object> validator) {
      this.validator = validator;
      return this;
    }

    public Definition build() {
      return new Definition(
          Objects.requireNonNull(name),
          Optional.ofNullable(defaultValue),
          Objects.requireNonNull(documentation),
          Objects.requireNonNull(type),
          validator);
    }
  }
}
