package org.astraea.web;

import com.google.gson.Gson;

interface JsonObject {
  default String json() {
    return new Gson().toJson(this);
  }
}
