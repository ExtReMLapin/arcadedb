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

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.math.SQLFunctionMathAbstract;

import java.util.*;

/**
 * Abstract class to find paths between nodes using heuristic .
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public abstract class SQLFunctionHeuristicPathFinderAbstract extends SQLFunctionMathAbstract {
  public static final    String PARAM_DIRECTION                = "direction";
  public static final    String PARAM_EDGE_TYPE_NAMES          = "edgeTypeNames";
  public static final    String PARAM_VERTEX_AXIS_NAMES        = "vertexAxisNames";
  public static final    String PARAM_PARALLEL                 = "parallel";
  public static final    String PARAM_MAX_DEPTH                = "maxDepth";
  public static final    String PARAM_HEURISTIC_FORMULA        = "heuristicFormula";
  public static final    String PARAM_CUSTOM_HEURISTIC_FORMULA = "customHeuristicFormula";
  public static final    String PARAM_D_FACTOR                 = "dFactor";
  public static final    String PARAM_TIE_BREAKER              = "tieBreaker";
  public static final    String PARAM_EMPTY_IF_MAX_DEPTH       = "emptyIfMaxDepth";
  protected static final Random rnd                            = new Random();

  protected Boolean             paramParallel               = false;
  protected Boolean             paramTieBreaker             = true;
  protected Boolean             paramEmptyIfMaxDepth        = false;
  protected String[]            paramEdgeTypeNames          = new String[] {};
  protected String[]            paramVertexAxisNames        = new String[] {};
  protected Vertex              paramSourceVertex;
  protected Vertex              paramDestinationVertex;
  protected SQLHeuristicFormula paramHeuristicFormula       = SQLHeuristicFormula.MANHATTAN;
  protected Vertex.DIRECTION    paramDirection              = Vertex.DIRECTION.OUT;
  protected long                paramMaxDepth               = Long.MAX_VALUE;
  protected double              paramDFactor                = 1.0;
  protected String              paramCustomHeuristicFormula = "";

  protected              CommandContext context;
  protected final        List<Vertex>   route = new LinkedList<Vertex>();
  protected static final float          MIN   = 0f;

  public SQLFunctionHeuristicPathFinderAbstract(final String iName) {
    super(iName);
  }

  // Approx Great-circle distance.  Args in degrees, result in kilometers
  // obtains from https://github.com/enderceylan/CS-314--Data-Structures/blob/master/HW10-Graph.java
  public double gcdist(final double lata, final double longa, final double latb, final double longb) {
    final double midlat;
    final double psi;
    final double dist;
    midlat = 0.5 * (lata + latb);
    psi = 0.0174532925 * Math.sqrt(Math.pow(lata - latb, 2) + Math.pow((longa - longb) * Math.cos(0.0174532925 * midlat), 2));
    dist = 6372.640112 * psi;
    return dist;
  }

  protected boolean isVariableEdgeWeight() {
    return false;
  }

  protected abstract double getDistance(final Vertex node, final Vertex parent, final Vertex target);

  protected abstract double getHeuristicCost(final Vertex node, final Vertex parent, final Vertex target, CommandContext context);

  protected LinkedList<Vertex> getPath() {
    final LinkedList<Vertex> path = new LinkedList<Vertex>(route);
    return path;
  }

  protected Set<Vertex> getNeighbors(final Vertex node) {
    context.incrementVariable("getNeighbors");

    final Set<Vertex> neighbors = new HashSet<Vertex>();
    if (node != null) {
      for (final Vertex v : node.getVertices(paramDirection, paramEdgeTypeNames)) {
        final Vertex ov = v;
        if (ov != null)
          neighbors.add(ov);
      }
    }
    return neighbors;
  }

  // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
  protected double getSimpleHeuristicCost(final double x, final double g, final double dFactor) {
    final double dx = Math.abs(x - g);
    return dFactor * (dx);
  }

  // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
  protected double getManhattanHeuristicCost(final double x, final double y, final double gx, final double gy, final double dFactor) {
    final double dx = Math.abs(x - gx);
    final double dy = Math.abs(y - gy);
    return dFactor * (dx + dy);
  }

  protected double getMaxAxisHeuristicCost(final double x, final double y, final double gx, final double gy, final double dFactor) {
    final double dx = Math.abs(x - gx);
    final double dy = Math.abs(y - gy);
    return dFactor * Math.max(dx, dy);
  }

  // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
  protected double getDiagonalHeuristicCost(final double x, final double y, final double gx, final double gy, final double dFactor) {
    final double dx = Math.abs(x - gx);
    final double dy = Math.abs(y - gy);
    final double h_diagonal = Math.min(dx, dy);
    final double h_straight = dx + dy;
    return (dFactor * 2) * h_diagonal + dFactor * (h_straight - 2 * h_diagonal);
  }

  // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
  protected double getEuclideanHeuristicCost(final double x, final double y, final double gx, final double gy, final double dFactor) {
    final double dx = Math.abs(x - gx);
    final double dy = Math.abs(y - gy);

    return (dFactor * Math.sqrt(Math.pow(dx, 2) + Math.pow(dy, 2)));
  }

  protected double getEuclideanNoSQRHeuristicCost(final double x, final double y, final double gx, final double gy, final double dFactor) {
    final double dx = Math.abs(x - gx);
    final double dy = Math.abs(y - gy);

    return (dFactor * (Math.pow(dx, 2) + Math.pow(dy, 2)));
  }

  // obtains from http://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
  protected double getTieBreakingHeuristicCost(final double x, final double y, final double sx, final double sy, final double gx, final double gy,
      double heuristic) {
    final double dx1 = x - gx;
    final double dy1 = y - gy;
    final double dx2 = sx - gx;
    final double dy2 = sy - gy;
    final double cross = Math.abs(dx1 * dy2 - dx2 * dy1);
    heuristic += (cross * 0.0001);
    return heuristic;
  }

  protected double getTieBreakingRandomHeuristicCost(final double x, final double y, final double sx, final double sy, final double gx, final double gy,
      double heuristic) {
    final double dx1 = x - gx;
    final double dy1 = y - gy;
    final double dx2 = sx - gx;
    final double dy2 = sy - gy;
    final double cross = Math.abs(dx1 * dy2 - dx2 * dy1) + rnd.nextFloat();
    heuristic += (cross * heuristic);
    return heuristic;
  }

  protected double getManhattanHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
      final Map<String, Double> plist, final Map<String, Double> glist, final long depth, final double dFactor) {
    final double heuristic;
    double res = 0.0;
    for (final String str : axisNames) {
      res += Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0));
    }
    heuristic = dFactor * res;
    return heuristic;
  }

  protected double getMaxAxisHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
      final Map<String, Double> plist, final Map<String, Double> glist, final long depth, final double dFactor) {
    final double heuristic;
    double res = 0.0;
    for (final String str : axisNames) {
      res = Math.max(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), res);
    }
    heuristic = dFactor * res;
    return heuristic;
  }

  protected double getDiagonalHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
      final Map<String, Double> plist, final Map<String, Double> glist, final long depth, final double dFactor) {

    final double heuristic;
    double h_diagonal = 0.0;
    double h_straight = 0.0;
    for (final String str : axisNames) {
      h_diagonal = Math.min(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), h_diagonal);
      h_straight += Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0));
    }
    heuristic = (dFactor * 2) * h_diagonal + dFactor * (h_straight - 2 * h_diagonal);
    return heuristic;
  }

  protected double getEuclideanHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
      final Map<String, Double> plist, final Map<String, Double> glist, final long depth, final double dFactor) {
    final double heuristic;
    double res = 0.0;
    for (final String str : axisNames) {
      res += Math.pow(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), 2);
    }
    heuristic = Math.sqrt(res);
    return heuristic;
  }

  protected double getEuclideanNoSQRHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
      final Map<String, Double> plist, final Map<String, Double> glist, final long depth, final double dFactor) {
    final double heuristic;
    double res = 0.0;
    for (final String str : axisNames) {
      res += Math.pow(Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0)), 2);
    }
    heuristic = dFactor * res;
    return heuristic;
  }

  protected double getTieBreakingHeuristicCost(final String[] axisNames, final Map<String, Double> slist, final Map<String, Double> clist,
      final Map<String, Double> plist, final Map<String, Double> glist, final long depth, double heuristic) {

    double res = 0.0;
    for (final String str : axisNames) {
      res += Math.abs((clist.get(str) != null ? clist.get(str) : 0.0) - (glist.get(str) != null ? glist.get(str) : 0.0));
    }
    final double cross = res;
    heuristic += (cross * 0.0001);
    return heuristic;
  }

  protected String[] stringArray(final Object fromObject) {
    if (fromObject == null) {
      return new String[] {};
    }
    if (fromObject instanceof String) {
      final String[] arr = fromObject.toString().replace("},{", " ,").split(",");
      return (arr);
    }
    if (fromObject instanceof Object) {
      return ((String[]) fromObject);
    }

    return new String[] {};
  }

  protected Boolean booleanOrDefault(final Object fromObject, final boolean defaultValue) {
    if (fromObject == null) {
      return defaultValue;
    }
    if (fromObject instanceof Boolean boolean1) {
      return boolean1;
    }
    if (fromObject instanceof String string) {
      return Boolean.parseBoolean(string);
    }
    return defaultValue;
  }

  protected String stringOrDefault(final Object fromObject, final String defaultValue) {
    if (fromObject == null) {
      return defaultValue;
    }
    return (String) fromObject;
  }

  protected Integer integerOrDefault(final Object fromObject, final int defaultValue) {
    if (fromObject == null) {
      return defaultValue;
    }
    if (fromObject instanceof Number number) {
      return number.intValue();
    }
    if (fromObject instanceof String) {
      try {
        return Integer.parseInt(fromObject.toString());
      } catch (final NumberFormatException ignore) {
      }
    }
    return defaultValue;
  }

  protected Long longOrDefault(final Object fromObject, final long defaultValue) {
    if (fromObject == null) {
      return defaultValue;
    }
    if (fromObject instanceof Number number) {
      return number.longValue();
    }
    if (fromObject instanceof String) {
      try {
        return Long.parseLong(fromObject.toString());
      } catch (final NumberFormatException ignore) {
      }
    }
    return defaultValue;
  }

  protected Double doubleOrDefault(final Object fromObject, final double defaultValue) {
    if (fromObject == null) {
      return defaultValue;
    }
    if (fromObject instanceof Number number) {
      return number.doubleValue();
    }
    if (fromObject instanceof String) {
      try {
        return Double.parseDouble(fromObject.toString());
      } catch (final NumberFormatException ignore) {
      }
    }
    return defaultValue;
  }

}
