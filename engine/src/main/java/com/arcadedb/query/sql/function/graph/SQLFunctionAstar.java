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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A*'s algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph with husrestic
 * function.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight' and fourth parameter represents the map of options.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0 .
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class SQLFunctionAstar extends SQLFunctionHeuristicPathFinderAbstract {
  public static final String NAME = "astar";

  private         String              paramWeightFieldName = "weight";
  private         long                currentDepth         = 0;
  protected final Set<Vertex>         closedSet            = new HashSet<Vertex>();
  protected final Map<Vertex, Vertex> cameFrom             = new HashMap<Vertex, Vertex>();

  protected final Map<Vertex, Double>   gScore = new HashMap<Vertex, Double>();
  protected final Map<Vertex, Double>   fScore = new HashMap<Vertex, Double>();
  protected final PriorityQueue<Vertex> open   = new PriorityQueue<Vertex>(1,
      (nodeA, nodeB) -> Double.compare(fScore.get(nodeA), fScore.get(nodeB)));

  public SQLFunctionAstar() {
    super(NAME);
  }

  public LinkedList<Vertex> execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext ctx) {
    context = ctx;
    final SQLFunctionAstar context = this;

    final Document record = currentRecord != null ? (Document) currentRecord.getRecord() : null;

    Object source = params[0];
    if (MultiValue.isMultiValue(source)) {
      if (MultiValue.getSize(source) > 1)
        throw new IllegalArgumentException("Only one sourceVertex is allowed");
      source = MultiValue.getFirstValue(source);
      if (source instanceof Result && ((Result) source).isElement()) {
        source = ((Result) source).getElement().get();
      }
    }

    if (record != null)
      source = record.get((String) source);

    if (source instanceof Identifiable) {
      final Document elem = (Document) ((Identifiable) source).getRecord();
      if (!(elem instanceof Vertex))
        throw new IllegalArgumentException("The sourceVertex must be a vertex record");

      paramSourceVertex = (Vertex) elem;
    } else {
      throw new IllegalArgumentException("The sourceVertex must be a vertex record");
    }

    Object dest = params[1];
    if (MultiValue.isMultiValue(dest)) {
      if (MultiValue.getSize(dest) > 1)
        throw new IllegalArgumentException("Only one destinationVertex is allowed");
      dest = MultiValue.getFirstValue(dest);
      if (dest instanceof Result result && result.isElement()) {
        dest = result.getElement().get();
      }
    }

    if (record != null)
      dest = record.get((String) dest);

    if (dest instanceof Identifiable identifiable) {
      final Document elem = (Document) identifiable.getRecord();
      if (!(elem instanceof Vertex vertex)) {
        throw new IllegalArgumentException("The destinationVertex must be a vertex record");
      }
      paramDestinationVertex = vertex;
    } else {
      throw new IllegalArgumentException("The destinationVertex must be a vertex record");
    }

    paramWeightFieldName = FileUtils.getStringContent(params[2]);

    if (params.length > 3) {
      bindAdditionalParams(params[3], context);
    }
    ctx.setVariable("getNeighbors", 0);
    if (paramSourceVertex == null || paramDestinationVertex == null) {
      return new LinkedList<>();
    }
    return internalExecute(ctx, ctx.getDatabase());

  }

  private LinkedList<Vertex> internalExecute(final CommandContext ctx, final Database graph) {

    final Vertex start = paramSourceVertex;
    final Vertex goal = paramDestinationVertex;

    open.add(start);

    // The cost of going from start to start is zero.
    gScore.put(start, 0.0);
    // For the first node, that value is completely heuristic.
    fScore.put(start, getHeuristicCost(start, null, goal, ctx));

    while (!open.isEmpty()) {
      Vertex current = open.poll();

      // we discussed about this feature in https://github.com/orientechnologies/orientdb/pull/6002#issuecomment-212492687
      if (paramEmptyIfMaxDepth && currentDepth >= paramMaxDepth) {
        route.clear(); // to ensure our result is empty
        return getPath();
      }
      // if start and goal vertex is equal so return current path from  cameFrom hash map
      if (current.getIdentity().equals(goal.getIdentity()) || currentDepth >= paramMaxDepth) {

        while (current != null) {
          route.add(0, current);
          current = cameFrom.get(current);
        }
        return getPath();
      }

      closedSet.add(current);
      for (final Edge neighborEdge : getNeighborEdges(current)) {

        final Vertex neighbor = getNeighbor(current, neighborEdge, graph);
        // Ignore the neighbor which is already evaluated.
        if (closedSet.contains(neighbor)) {
          continue;
        }
        // The distance from start to a neighbor
        final double tentative_gScore = gScore.get(current) + getDistance(neighborEdge);
        final boolean contains = open.contains(neighbor);

        if (!contains || tentative_gScore < gScore.get(neighbor)) {
          gScore.put(neighbor, tentative_gScore);
          fScore.put(neighbor, tentative_gScore + getHeuristicCost(neighbor, current, goal, ctx));

          if (contains) {
            open.remove(neighbor);
          }
          open.offer(neighbor);
          cameFrom.put(neighbor, current);
        }
      }

      // Increment Depth Level
      currentDepth++;

    }

    return getPath();
  }

  private Vertex getNeighbor(final Vertex current, final Edge neighborEdge, final Database graph) {
    if (neighborEdge.getOut().equals(current.getIdentity())) {
      return toVertex(neighborEdge.getIn());
    }
    return toVertex(neighborEdge.getOut());
  }

  private Vertex toVertex(Identifiable outVertex) {
    if (outVertex == null)
      return null;

    if (!(outVertex instanceof Record))
      outVertex = outVertex.getRecord();

    return (Vertex) outVertex;
  }

  protected Set<Edge> getNeighborEdges(final Vertex node) {
    context.incrementVariable("getNeighbors");

    final Set<Edge> neighbors = new HashSet<Edge>();
    if (node != null) {
      for (final Edge v : node.getEdges(paramDirection, paramEdgeTypeNames)) {
        final Edge ov = v;
        if (ov != null)
          neighbors.add(ov);
      }
    }
    return neighbors;
  }

  private void bindAdditionalParams(final Object additionalParams, final SQLFunctionAstar context) {
    if (additionalParams == null) {
      return;
    }
    Map<String, Object> mapParams = null;
    if (additionalParams instanceof Map) {
      mapParams = (Map) additionalParams;
    } else if (additionalParams instanceof Identifiable) {
      mapParams = ((Document) ((Identifiable) additionalParams).getRecord()).toMap();
    }
    if (mapParams != null) {
      context.paramEdgeTypeNames = stringArray(mapParams.get(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES));
      context.paramVertexAxisNames = stringArray(mapParams.get(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES));
      if (mapParams.get(SQLFunctionAstar.PARAM_DIRECTION) != null) {
        if (mapParams.get(SQLFunctionAstar.PARAM_DIRECTION) instanceof String) {
          context.paramDirection = Vertex.DIRECTION.valueOf(
              stringOrDefault(mapParams.get(SQLFunctionAstar.PARAM_DIRECTION), "OUT").toUpperCase(Locale.ENGLISH));
        } else {
          context.paramDirection = (Vertex.DIRECTION) mapParams.get(SQLFunctionAstar.PARAM_DIRECTION);
        }
      }

      context.paramParallel = booleanOrDefault(mapParams.get(SQLFunctionAstar.PARAM_PARALLEL), false);
      context.paramMaxDepth = longOrDefault(mapParams.get(SQLFunctionAstar.PARAM_MAX_DEPTH), context.paramMaxDepth);
      context.paramEmptyIfMaxDepth = booleanOrDefault(mapParams.get(SQLFunctionAstar.PARAM_EMPTY_IF_MAX_DEPTH),
          context.paramEmptyIfMaxDepth);
      context.paramTieBreaker = booleanOrDefault(mapParams.get(SQLFunctionAstar.PARAM_TIE_BREAKER), context.paramTieBreaker);
      context.paramDFactor = doubleOrDefault(mapParams.get(SQLFunctionAstar.PARAM_D_FACTOR), context.paramDFactor);
      if (mapParams.get(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA) != null) {
        if (mapParams.get(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA) instanceof String) {
          context.paramHeuristicFormula = SQLHeuristicFormula.valueOf(
              stringOrDefault(mapParams.get(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA), "MANHATTAN").toUpperCase(Locale.ENGLISH));
        } else {
          context.paramHeuristicFormula = (SQLHeuristicFormula) mapParams.get(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA);
        }
      }

      context.paramCustomHeuristicFormula = stringOrDefault(mapParams.get(SQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA), "");
    }
  }

  public String getSyntax() {
    return "astar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
  }

  @Override
  public Object getResult() {
    return getPath();
  }

  @Override
  protected double getDistance(final Vertex node, final Vertex parent, final Vertex target) {
    final Iterator<Edge> edges = node.getEdges(paramDirection).iterator();
    Edge e = null;
    while (edges.hasNext()) {
      final Edge next = edges.next();
      if (next.getOut().equals(target.getIdentity()) || next.getIn().equals(target.getIdentity())) {
        e = next;
        break;
      }
    }
    if (e != null) {
      final Object fieldValue = e.get(paramWeightFieldName);
      if (fieldValue != null)
        if (fieldValue instanceof Float)
          return (Float) fieldValue;
        else if (fieldValue instanceof Number)
          return ((Number) fieldValue).doubleValue();
    }

    return MIN;
  }

  protected double getDistance(final Edge edge) {
    if (edge != null) {
      final Object fieldValue = edge.get(paramWeightFieldName);
      if (fieldValue != null)
        if (fieldValue instanceof Float)
          return (Float) fieldValue;
        else if (fieldValue instanceof Number)
          return ((Number) fieldValue).doubleValue();
    }

    return MIN;
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  protected double getHeuristicCost(final Vertex node, Vertex parent, final Vertex target, final CommandContext ctx) {
    double hresult = 0.0;

    if (paramVertexAxisNames.length == 0) {
      return hresult;
    } else if (paramVertexAxisNames.length == 1) {
      final double n = doubleOrDefault(node.get(paramVertexAxisNames[0]), 0.0);
      final double g = doubleOrDefault(target.get(paramVertexAxisNames[0]), 0.0);
      hresult = getSimpleHeuristicCost(n, g, paramDFactor);
    } else if (paramVertexAxisNames.length == 2) {
      if (parent == null)
        parent = node;
      final double sx = doubleOrDefault(paramSourceVertex.get(paramVertexAxisNames[0]), 0);
      final double sy = doubleOrDefault(paramSourceVertex.get(paramVertexAxisNames[1]), 0);
      final double nx = doubleOrDefault(node.get(paramVertexAxisNames[0]), 0);
      final double ny = doubleOrDefault(node.get(paramVertexAxisNames[1]), 0);
      final double px = doubleOrDefault(parent.get(paramVertexAxisNames[0]), 0);
      final double py = doubleOrDefault(parent.get(paramVertexAxisNames[1]), 0);
      final double gx = doubleOrDefault(target.get(paramVertexAxisNames[0]), 0);
      final double gy = doubleOrDefault(target.get(paramVertexAxisNames[1]), 0);

      switch (paramHeuristicFormula) {
      case MANHATTAN:
        hresult = getManhattanHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case MAXAXIS:
        hresult = getMaxAxisHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case DIAGONAL:
        hresult = getDiagonalHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case EUCLIDEAN:
        hresult = getEuclideanHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case EUCLIDEANNOSQR:
        hresult = getEuclideanNoSQRHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(px, py, sx, sy, gx, gy, hresult);
      }

    } else {
      final Map<String, Double> sList = new HashMap<String, Double>();
      final Map<String, Double> cList = new HashMap<String, Double>();
      final Map<String, Double> pList = new HashMap<String, Double>();
      final Map<String, Double> gList = new HashMap<String, Double>();
      parent = parent == null ? node : parent;
      for (int i = 0; i < paramVertexAxisNames.length; i++) {
        final Double s = doubleOrDefault(paramSourceVertex.get(paramVertexAxisNames[i]), 0);
        final Double c = doubleOrDefault(node.get(paramVertexAxisNames[i]), 0);
        final Double g = doubleOrDefault(target.get(paramVertexAxisNames[i]), 0);
        final Double p = doubleOrDefault(parent.get(paramVertexAxisNames[i]), 0);
        if (s != null)
          sList.put(paramVertexAxisNames[i], s);
        if (c != null)
          cList.put(paramVertexAxisNames[i], s);
        if (g != null)
          gList.put(paramVertexAxisNames[i], g);
        if (p != null)
          pList.put(paramVertexAxisNames[i], p);
      }
      switch (paramHeuristicFormula) {
      case MANHATTAN:
        hresult = getManhattanHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case MAXAXIS:
        hresult = getMaxAxisHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case DIAGONAL:
        hresult = getDiagonalHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case EUCLIDEAN:
        hresult = getEuclideanHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case EUCLIDEANNOSQR:
        hresult = getEuclideanNoSQRHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, hresult);
      }

    }

    return hresult;

  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }

}
