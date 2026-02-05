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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.ast.*;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for FOREACH clause.
 * FOREACH iterates over a list and executes updating clauses for each element.
 * Unlike UNWIND, FOREACH does not change the cardinality of rows - it only executes side effects.
 * <p>
 * Examples:
 * - FOREACH (i IN [1, 2, 3] | CREATE (:Item {id: i}))
 * - FOREACH (node IN nodes | SET node.processed = true)
 * - CREATE (root) FOREACH (i IN [1, 2, 3] | CREATE (root)-[:HAS_ITEM]->(:Item {id: i}))
 * <p>
 * The FOREACH clause:
 * - Evaluates the list expression for each input row
 * - For each element in the list, creates a temporary context with the loop variable
 * - Executes all nested clauses (CREATE, SET, DELETE, REMOVE, MERGE) for that element
 * - After processing all elements, passes through the original input row unchanged
 * - If the list is empty or null, the input row passes through without any operations
 */
public class ForeachStep extends AbstractExecutionStep {
  private final ForeachClause foreachClause;
  private final ExpressionEvaluator evaluator;
  private final DatabaseInternal database;

  public ForeachStep(final ForeachClause foreachClause, final CommandContext context,
                     final CypherFunctionFactory functionFactory) {
    super(context);
    this.foreachClause = foreachClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
    this.database = (DatabaseInternal) context.getDatabase();
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // If no previous step, create a single empty result
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;

        if (finished)
          return false;

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Initialize prevResults on first call
        if (prevResults == null) {
          if (hasPrevious) {
            prevResults = prev.syncPull(context, nRecords);
          } else {
            // No previous step - create a single empty input row
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed)
                  throw new NoSuchElementException();
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        // Process up to n input rows
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputRow = prevResults.next();

          // Evaluate the list expression
          final Expression listExpr = foreachClause.getListExpression();
          final Object listValue = evaluator.evaluate(listExpr, inputRow, context);

          // Convert to collection
          Collection<?> collection = null;
          if (listValue instanceof Collection) {
            collection = (Collection<?>) listValue;
          } else if (listValue instanceof Iterable) {
            final List<Object> list = new ArrayList<>();
            for (final Object obj : (Iterable<?>) listValue)
              list.add(obj);
            collection = list;
          } else if (listValue != null && listValue.getClass().isArray()) {
            // Convert array to list
            final List<Object> list = new ArrayList<>();
            if (listValue instanceof Object[]) {
              for (final Object obj : (Object[]) listValue)
                list.add(obj);
            } else if (listValue instanceof int[]) {
              for (final int i : (int[]) listValue)
                list.add(i);
            } else if (listValue instanceof long[]) {
              for (final long i : (long[]) listValue)
                list.add(i);
            } else if (listValue instanceof double[]) {
              for (final double i : (double[]) listValue)
                list.add(i);
            } else if (listValue instanceof boolean[]) {
              for (final boolean i : (boolean[]) listValue)
                list.add(i);
            }
            collection = list;
          }

          // Execute nested clauses for each element
          if (collection != null) {
            for (final Object element : collection) {
              // Create a temporary context with the loop variable
              final ResultInternal tempResult = new ResultInternal();

              // Copy all properties from input row
              for (final String prop : inputRow.getPropertyNames())
                tempResult.setProperty(prop, inputRow.getProperty(prop));

              // Add the loop variable
              tempResult.setProperty(foreachClause.getVariable(), element);

              // Execute each nested clause
              for (final ClauseEntry clauseEntry : foreachClause.getNestedClauses()) {
                executeNestedClause(clauseEntry, tempResult, context);
              }
            }
          }

          // Pass through the original input row unchanged
          buffer.add(inputRow);
        }

        if (!prevResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        ForeachStep.this.close();
      }
    };
  }

  /**
   * Executes a single nested clause within the FOREACH loop.
   */
  private void executeNestedClause(final ClauseEntry clauseEntry, final Result loopContext, final CommandContext context) {
    switch (clauseEntry.getType()) {
      case CREATE:
        final CreateClause createClause = clauseEntry.getTypedClause();
        executeCreate(createClause, loopContext, context);
        break;

      case SET:
        final SetClause setClause = clauseEntry.getTypedClause();
        executeSet(setClause, loopContext, context);
        break;

      case DELETE:
        final DeleteClause deleteClause = clauseEntry.getTypedClause();
        executeDelete(deleteClause, loopContext, context);
        break;

      case REMOVE:
        final RemoveClause removeClause = clauseEntry.getTypedClause();
        executeRemove(removeClause, loopContext, context);
        break;

      case MERGE:
        final MergeClause mergeClause = clauseEntry.getTypedClause();
        executeMerge(mergeClause, loopContext, context);
        break;

      default:
        // Reading clauses (MATCH, WITH, RETURN) are not allowed in FOREACH
        break;
    }
  }

  private void executeCreate(final CreateClause createClause, final Result loopContext, final CommandContext context) {
    // Delegate to CreateStep logic
    final CreateStep createStep = new CreateStep(createClause, context);
    // Create a mini result set with just the loop context
    final ResultSet miniResultSet = new ResultSet() {
      private boolean consumed = false;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public Result next() {
        if (consumed)
          throw new NoSuchElementException();
        consumed = true;
        return loopContext;
      }

      @Override
      public void close() {
      }
    };

    // Execute the create step
    final ResultSet createResults = createStep.syncPull(context, 1);
    // Consume the results (side effects have been performed)
    while (createResults.hasNext())
      createResults.next();
    createResults.close();
  }

  private void executeSet(final SetClause setClause, final Result loopContext, final CommandContext context) {
    // Execute SET operations using the loop context
    for (final SetClause.SetItem item : setClause.getItems()) {
      final Object target = loopContext.getProperty(item.getVariable());
      if (target != null) {
        final Object value = evaluator.evaluate(item.getValueExpression(), loopContext, context);
        // Set the property on the target element
        if (target instanceof com.arcadedb.graph.MutableVertex) {
          ((com.arcadedb.graph.MutableVertex) target).set(item.getProperty(), value);
        } else if (target instanceof com.arcadedb.graph.MutableEdge) {
          ((com.arcadedb.graph.MutableEdge) target).set(item.getProperty(), value);
        }
      }
    }
  }

  private void executeDelete(final DeleteClause deleteClause, final Result loopContext, final CommandContext context) {
    // Delegate to DeleteStep logic
    final DeleteStep deleteStep = new DeleteStep(deleteClause, context);
    final ResultSet miniResultSet = new ResultSet() {
      private boolean consumed = false;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public Result next() {
        if (consumed)
          throw new NoSuchElementException();
        consumed = true;
        return loopContext;
      }

      @Override
      public void close() {
      }
    };

    // Execute the delete step
    deleteStep.setPrevious(new AbstractExecutionStep(context) {
      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        return miniResultSet;
      }

      @Override
      public String prettyPrint(final int depth, final int indent) {
        return "";
      }
    });
    final ResultSet deleteResults = deleteStep.syncPull(context, 1);
    while (deleteResults.hasNext())
      deleteResults.next();
    deleteResults.close();
  }

  private void executeRemove(final RemoveClause removeClause, final Result loopContext, final CommandContext context) {
    // Execute REMOVE operations using the loop context
    for (final RemoveClause.RemoveItem item : removeClause.getItems()) {
      final Object target = loopContext.getProperty(item.getVariable());
      if (target != null && item.getProperty() != null) {
        // Remove the property from the target element
        if (target instanceof com.arcadedb.graph.MutableVertex) {
          ((com.arcadedb.graph.MutableVertex) target).remove(item.getProperty());
        } else if (target instanceof com.arcadedb.graph.MutableEdge) {
          ((com.arcadedb.graph.MutableEdge) target).remove(item.getProperty());
        }
      }
    }
  }

  private void executeMerge(final MergeClause mergeClause, final Result loopContext, final CommandContext context) {
    // Delegate to MergeStep logic
    final MergeStep mergeStep = new MergeStep(mergeClause, context, evaluator.getFunctionFactory());
    mergeStep.setPrevious(new AbstractExecutionStep(context) {
      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        return new ResultSet() {
          private boolean consumed = false;

          @Override
          public boolean hasNext() {
            return !consumed;
          }

          @Override
          public Result next() {
            if (consumed)
              throw new NoSuchElementException();
            consumed = true;
            return loopContext;
          }

          @Override
          public void close() {
          }
        };
      }

      @Override
      public String prettyPrint(final int depth, final int indent) {
        return "";
      }
    });
    final ResultSet mergeResults = mergeStep.syncPull(context, 1);
    while (mergeResults.hasNext())
      mergeResults.next();
    mergeResults.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FOREACH ");
    builder.append(foreachClause.getVariable());
    builder.append(" IN ");
    builder.append(foreachClause.getListExpression().getText());
    builder.append(" | <").append(foreachClause.getNestedClauses().size()).append(" clauses>");

    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");

    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
