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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.LocalEdgeType;

import java.util.*;
import java.util.stream.*;

/**
 * <p>This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the resulting graph is still
 * consistent </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  public RemoveEdgePointersStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final ResultInternal elem = (ResultInternal) upstream.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {

          final Set<String> propNames = elem.getPropertyNames();
          for (final String propName : propNames.stream().filter(x -> x.startsWith("in_") || x.startsWith("out_")).collect(Collectors.toList())) {
            final Object val = elem.getProperty(propName);
            if (val instanceof Document document) {
              if (document.getType() instanceof LocalEdgeType) {
                elem.removeProperty(propName);
              }
            } else if (val instanceof Iterable iterable) {
              for (final Object o : iterable) {
                if (o instanceof Document document) {
                  if (document.getType() instanceof LocalEdgeType) {
                    elem.removeProperty(propName);
                    break;
                  }
                }
              }
            }
          }
        } finally {
          if( context.isProfiling() ) {
            cost += (System.nanoTime() - begin);
          }
        }
        return elem;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if( context.isProfiling() ) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }
}
