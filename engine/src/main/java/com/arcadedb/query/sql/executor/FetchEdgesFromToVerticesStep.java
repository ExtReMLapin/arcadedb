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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.Identifier;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created by luigidellaquila on 21/02/17.
 */
public class FetchEdgesFromToVerticesStep extends AbstractExecutionStep {
  private final Identifier targetType;
  private final Identifier targetCluster;
  private final String     fromAlias;
  private final String     toAlias;

  //operation stuff

  //iterator of FROM vertices
  Iterator       fromIter;
  //iterator of edges on current from
  Iterator<Edge> currentFromEdgesIter;

  final   Set<RID> toList = new HashSet<>();
  private boolean  inited = false;

  private Edge nextEdge = null;

  public FetchEdgesFromToVerticesStep(final String fromAlias, final String toAlias, final Identifier targetType,
      final Identifier targetCluster, final CommandContext context) {
    super(context);
    this.targetType = targetType;
    this.targetCluster = targetCluster;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    init();
    return new ResultSet() {
      int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && nextEdge != null);
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Edge edge = nextEdge;
        fetchNextEdge();
        final ResultInternal result = new ResultInternal(edge);
        currentBatch++;
        return result;
      }

      @Override
      public void close() {
        if (fromIter instanceof ResultSet set) {
          set.close();
        }
      }
    };
  }

  private void init() {
    synchronized (this) {
      if (this.inited)
        return;
      inited = true;
    }

    Object fromValues;

    fromValues = context.getVariable(fromAlias);
    if (fromValues != null)
      if (fromValues instanceof Iterable iterable && !(fromValues instanceof Identifiable))
        fromValues = iterable.iterator();
      else if (!(fromValues instanceof Iterator))
        fromValues = Set.of(fromValues).iterator();

    Object toValues;

    toValues = context.getVariable(toAlias);
    if (toValues != null)
      if (toValues instanceof Iterable iterable && !(toValues instanceof Identifiable))
        toValues = iterable.iterator();
      else if (!(toValues instanceof Iterator))
        toValues = Set.of(toValues).iterator();

    fromIter = (Iterator) fromValues;

    final Iterator toIter = (Iterator) toValues;

    while (toIter != null && toIter.hasNext()) {
      Object elem = toIter.next();
      if (elem instanceof Result result)
        elem = result.toElement();

      if (elem instanceof Identifiable identifiable && !(elem instanceof Record))
        elem = identifiable.getRecord();

      if (!(elem instanceof Record))
        throw new CommandExecutionException("Invalid vertex: " + elem);

      if (elem instanceof Vertex vertex)
        toList.add(vertex.getIdentity());
    }

    fetchNextEdge();
  }

  private void fetchNextEdge() {
    this.nextEdge = null;
    while (true) {
      while (this.currentFromEdgesIter == null || !this.currentFromEdgesIter.hasNext()) {
        if (this.fromIter == null) {
          return;
        }
        if (this.fromIter.hasNext()) {
          Object from = fromIter.next();
          if (from instanceof Result result) {
            from = result.toElement();
          }
          if (from instanceof Identifiable identifiable && !(from instanceof Record)) {
            from = identifiable.getRecord();
          }
          if (from instanceof Vertex vertex) {

            // TODO: SUPPORT GET EDGE WITH 'TO' AS PARAMETER
            currentFromEdgesIter = vertex.getEdges(Vertex.DIRECTION.OUT).iterator();
          } else {
            throw new CommandExecutionException("Invalid vertex: " + from);
          }
        } else {
          return;
        }
      }
      final Edge edge = this.currentFromEdgesIter.next();
      if (toList.isEmpty() || toList.contains(edge.getIn().getIdentity())) {
        if (matchesClass(edge) && matchesBucket(edge)) {
          this.nextEdge = edge;
          return;
        }
      }
    }
  }

  private boolean matchesBucket(final Edge edge) {
    if (targetCluster == null)
      return true;

    final int bucketId = edge.getIdentity().getBucketId();
    final String bucketName = context.getDatabase().getSchema().getBucketById(bucketId).getName();
    return bucketName.equals(targetCluster.getStringValue());
  }

  private boolean matchesClass(final Edge edge) {
    if (targetType == null)
      return true;
    return edge.getType().isSubTypeOf(targetType.getStringValue());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES FROM x TO y";
    if (targetType != null)
      result += "\n" + spaces + "       (target class " + targetType + ")";

    if (targetCluster != null)
      result += "\n" + spaces + "       (target bucket " + targetCluster + ")";

    return result;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FetchEdgesFromToVerticesStep(fromAlias, toAlias, targetType, targetCluster, context);
  }
}
