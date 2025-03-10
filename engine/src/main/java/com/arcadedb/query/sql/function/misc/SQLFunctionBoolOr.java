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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionConfigurableAbstract;

/**
 * Computes the aggregate "or" over a field.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionBoolOr extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "bool_or";

  private Boolean or;

  public SQLFunctionBoolOr() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length == 1) {
      if (params[0] instanceof Boolean boolean1)
        or(boolean1);
      else if (MultiValue.isMultiValue(params[0]))
        for (final Object n : MultiValue.getMultiValueIterable(params[0])) {
          or((Boolean) n);
          if (or) break;
        }
    } else {
      or = null;
      for (int i = 0; i < params.length; ++i) {
        or((Boolean) params[i]);
        if (or) break;
      }
    }
    return or;
  }

  protected void or(final Boolean value) {
    if (value != null) {
      if (or == null)
        // FIRST TIME
        or = value;
      else
        or = or || value;
    }
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  public String getSyntax() {
    return "bool_or(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return or == null ? false : or;
  }
}
