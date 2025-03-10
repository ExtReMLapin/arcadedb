/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.SystemVariableResolver;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Represents a context configuration where custom setting could be defined for the context only. If not defined, globals will be
 * taken.
 **/
public class ContextConfiguration implements Serializable {
  private final           Map<String, Object>    config         = new ConcurrentHashMap<String, Object>();
  private transient final SystemVariableResolver customResolver = new SystemVariableResolver() {
    @Override
    public String resolve(final String variable) {
      Object result = config.get(variable);
      if (result == null)
        result = super.resolve(variable);
      return result != null ? result.toString() : null;
    }
  };

  /**
   * Empty constructor to create just a proxy for the GlobalConfiguration. No values are set.
   */
  public ContextConfiguration() {
  }

  /**
   * Initializes the context with custom parameters.
   *
   * @param iConfig Map of parameters of type {@literal Map<String, Object>}.
   */
  public ContextConfiguration(final Map<String, Object> iConfig) {
    this.config.putAll(iConfig);
  }

  public ContextConfiguration(final ContextConfiguration iParent) {
    if (iParent != null)
      config.putAll(iParent.config);
  }

  public void fromJSON(final String input) {
    if (input == null)
      return;

    final JSONObject json = new JSONObject(input);

    final JSONObject cfg = json.getJSONObject("configuration");
    for (final String k : cfg.keySet()) {
      final GlobalConfiguration cfgEntry = GlobalConfiguration.findByKey(GlobalConfiguration.PREFIX + k);
      if (cfgEntry != null) {
        config.put(GlobalConfiguration.PREFIX + k, cfg.get(k));
      }
    }
  }

  public String toJSON() {
    final JSONObject json = new JSONObject();

    final JSONObject cfg = new JSONObject();
    json.put("configuration", cfg);

    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      cfg.put(entry.getKey().substring(GlobalConfiguration.PREFIX.length()), entry.getValue());
    }

    return json.toString();
  }

  public Object setValue(final GlobalConfiguration iConfig, final Object iValue) {
    if (iValue == null)
      return config.remove(iConfig.getKey());

    return config.put(iConfig.getKey(), iValue);
  }

  public Object setValue(final String iName, final Object iValue) {
    if (iValue == null)
      return config.remove(iName);

    return config.put(iName, iValue);
  }

  public <T> T getValue(final GlobalConfiguration iConfig) {
    if (config.containsKey(iConfig.getKey()))
      return (T) config.get(iConfig.getKey());
    return iConfig.getValue();
  }

  /**
   * @param config Global configuration parameter.
   *
   * @return Value of configuration parameter stored in this context as enumeration if such one exists, otherwise value stored in
   * passed in {@link GlobalConfiguration} instance.
   *
   * @throws ClassCastException       if stored value can not be casted and parsed from string to passed in enumeration class.
   * @throws IllegalArgumentException if value associated with configuration parameter is a string bug can not be converted to
   *                                  instance of passed in enumeration class.
   */
  public <T extends Enum<T>> T getValueAsEnum(final GlobalConfiguration config, final Class<T> enumType) {
    final Object value;
    if (this.config.containsKey(config.getKey())) {
      value = this.config.get(config.getKey());
    } else {
      value = config.getValue();
    }

    if (value == null)
      return null;

    if (enumType.isAssignableFrom(value.getClass())) {
      return enumType.cast(value);
    } else if (value instanceof String) {
      final String presentation = value.toString().toUpperCase(Locale.ENGLISH);
      return Enum.valueOf(enumType, presentation);
    } else {
      throw new ClassCastException("Value " + value + " can not be cast to enumeration " + enumType.getSimpleName());
    }
  }

  public boolean hasValue(final String iName) {
    return config.containsKey(iName);
  }

  @SuppressWarnings("unchecked")
  public <T> T getValue(final String iName, final T defaultValue) {
    if (config.containsKey(iName))
      return (T) config.get(iName);

    final String sysProperty = System.getProperty(iName);
    if (sysProperty != null)
      return (T) sysProperty;

    return defaultValue;
  }

  public boolean getValueAsBoolean(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return false;
    return v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
  }

  public String getValueAsString(final String iName, final String iDefaultValue) {
    return getValue(iName, iDefaultValue);
  }

  public String getValueAsString(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return null;

    return getVariable(v.toString(), "");
  }

  public int getValueAsInteger(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Integer i ? i : Integer.parseInt(v.toString());
  }

  public long getValueAsLong(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Long l ? l : Long.parseLong(v.toString());
  }

  public float getValueAsFloat(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Float f ? f : Float.parseFloat(v.toString());
  }

  public Set<String> getContextKeys() {
    return config.keySet();
  }

  public void merge(final ContextConfiguration contextConfiguration) {
    this.config.putAll(contextConfiguration.config);
  }

  public void reset() {
    config.clear();
  }

  private String getVariable(final String name, final String defValue) {
    String result = customResolver.resolveSystemVariables(name, defValue);
    if (result == null)
      result = defValue;
    return result;
  }
}
