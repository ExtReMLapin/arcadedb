/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.schema.Type;

import java.util.*;
import java.util.logging.*;

/**
 * Converts a value to another type in Java or ArcadeDB's supported types.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodConvert extends AbstractSQLMethod {

  public static final String NAME = "convert";

  public SQLMethodConvert() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "convert(<type>)";
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    if (value == null || params[0] == null) {
      return null;
    }

    final String destType = params[0].toString();

    if (destType.contains(".")) {
      try {
        return Type.convert(context.getDatabase(), value, Class.forName(destType));
      } catch (final ClassNotFoundException e) {
        LogManager.instance().log(this, Level.SEVERE, "Type for destination type was not found", e);
      }
    } else {
      final Type arcadeType = Type.valueOf(destType.toUpperCase(Locale.ENGLISH));
      if (arcadeType != null) {
        return Type.convert(context.getDatabase(), value, arcadeType.getDefaultJavaType());
      }
    }

    return null;
  }
}
