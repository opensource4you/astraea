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
import org.apache.kafka.common.config.ConfigDef;

public interface Definition {

  static ConfigDef toConfigDef(Collection<Definition> defs) {
    var def = new ConfigDef();
    defs.forEach(
        d ->
            def.define(
                d.name(),
                ConfigDef.Type.valueOf(d.type().name()),
                d.defaultValue(),
                ConfigDef.Importance.MEDIUM,
                d.documentation()));
    return def;
  }

  String name();

  Object defaultValue();

  String documentation();

  Type type();

  enum Type {
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

  class Builder {
    private String name;
    private Object defaultValue;

    private String documentation = "";
    private Type type;

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
      return this;
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

    public Definition build() {
      return new Definition() {
        private final String name = Objects.requireNonNull(Builder.this.name);
        private final String documentation = Objects.requireNonNull(Builder.this.documentation);
        private final Object defaultValue = Builder.this.defaultValue;
        private final Type type = Objects.requireNonNull(Builder.this.type);

        @Override
        public String name() {
          return name;
        }

        @Override
        public Object defaultValue() {
          return defaultValue;
        }

        @Override
        public String documentation() {
          return documentation;
        }

        @Override
        public Type type() {
          return type;
        }
      };
    }
  }
}
