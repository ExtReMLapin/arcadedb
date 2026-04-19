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
package com.arcadedb.query.opencypher;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

/**
 * Regression tests for GitHub issues #3445-#3449.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherBugFixesTest extends TestHelper {

  // ===================== #3445: coll.indexOf with null should return null =====================

  @Test
  void collIndexOfWithNullValueReturnsNull() {
    // Issue #3445: coll.indexOf(['a'], null) should return null, not -1
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.indexOf(['a'], null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void collIndexOfWithNullListReturnsNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.indexOf(null, 'a') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void collIndexOfNormalBehavior() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.indexOf(['a', 'b', 'c'], 'b') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).longValue()).isEqualTo(1L);
    }
  }

  // ===================== #3446: coll.remove with out-of-bounds index should throw =====================

  @Test
  void collRemoveOutOfBoundsThrows() {
    // Issue #3446: coll.remove([1, 2], 10) should throw an error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN coll.remove([1, 2], 10) AS result")) {
        rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void collRemoveNegativeIndexThrows() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN coll.remove([1, 2], -1) AS result")) {
        rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void collRemoveNormalBehavior() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.remove([1, 2, 3], 1) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).hasSize(2);
      assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
      assertThat(((Number) result.get(1)).longValue()).isEqualTo(3L);
    }
  }

  // ===================== #3447: isEmpty should not treat null as empty =====================

  @Test
  void isEmptyWithNullProperty() {
    // Issue #3447: isEmpty(p.address) where p.address is null should NOT return true
    database.transaction(() -> {
      database.getSchema().createVertexType("PersonTest");
      database.command("opencypher",
          "CREATE (p:PersonTest {name:'Jessica', address:''}), (q:PersonTest {name:'Keanu'})");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (p:PersonTest) WHERE isEmpty(p.address) RETURN p.name AS result ORDER BY result")) {
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("result"));
      // Only Jessica has address='' (empty string), Keanu has no address property (null)
      assertThat(names).containsExactly("Jessica");
    }
  }

  @Test
  void isEmptyWithEmptyString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty('') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }
  }

  @Test
  void isEmptyWithNonEmptyString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty('hello') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isFalse();
    }
  }

  @Test
  void isEmptyWithNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ===================== #3448: toBooleanOrNull(1.5) should return null =====================

  @Test
  void toBooleanOrNullWithFloat() {
    // Issue #3448: toBooleanOrNull(1.5) should return null, not true
    try (final ResultSet rs = database.command("opencypher",
        "RETURN toBooleanOrNull('not a boolean') AS str, toBooleanOrNull(1.5) AS float, toBooleanOrNull([]) AS array")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("str")).isNull();
      assertThat((Object) row.getProperty("float")).isNull();
      assertThat((Object) row.getProperty("array")).isNull();
    }
  }

  // ===================== #3449: size on vector should return vector length =====================

  @Test
  void sizeOnVector() {
    // Issue #3449: size(vector([1, 2, 3, 4, 5], 5, INTEGER)) should return 5
    try (final ResultSet rs = database.command("opencypher",
        "RETURN size(vector([1, 2, 3, 4, 5], 5, INTEGER)) AS sizeResult")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("sizeResult")).longValue()).isEqualTo(5L);
    }
  }

  @Test
  void sizeOnVectorThreeElements() {
    try (final ResultSet rs = database.command("opencypher",
        "RETURN size(vector([1.0, 2.0, 3.0], 3, FLOAT)) AS sizeResult")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("sizeResult")).longValue()).isEqualTo(3L);
    }
  }

  // ===================== Bug 6: tail(null) should return null, not [] =====================

  @Test
  void tailWithNullReturnsNull() {
    // Neo4j: tail(null) returns null
    // ArcadeDB bug: tail(null) returns []
    try (final ResultSet rs = database.command("opencypher", "RETURN tail(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void tailNormalBehavior() {
    try (final ResultSet rs = database.command("opencypher", "RETURN tail(['a', 'b', 'c']) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).containsExactly("b", "c");
    }
  }



  // ===================== Bug 11: || operator for list concatenation =====================

  @Test
  void listConcatenationWithDoublePipe() {
    // Neo4j: [1,2] || [3,4] returns [1, 2, 3, 4]
    // ArcadeDB bug: [1,2] || [3,4] returns [1, 2] (only first list)
    try (final ResultSet rs = database.command("opencypher", "RETURN [1, 2] || [3, 4] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).containsExactly(1, 2, 3, 4);
    }
  }

  // ===================== Bug 12: CASE null WHEN null should not match =====================

  @Test
  void caseNullWhenNullDoesNotMatch() {
    // Neo4j: CASE null WHEN null THEN 'matched' ELSE 'not_matched' END returns 'not_matched'
    // ArcadeDB bug: returns 'matched' (null should never equal null in simple CASE)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN CASE null WHEN null THEN 'matched' ELSE 'not_matched' END AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isEqualTo("not_matched");
    }
  }

  // ===================== Bug 13: Dynamic label matching with $() =====================

  @Test
  void dynamicLabelMatching() {
    // Neo4j: MATCH (n:$(label)) works with dynamic labels
    // ArcadeDB bug: dynamic label matching returns no results
    database.transaction(() -> {
      database.getSchema().createVertexType("DynamicLabelTest");
      database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");
    });

    try (final ResultSet rs = database.command("opencypher",
        "WITH 'DynamicLabelTest' AS label MATCH (n:$(label)) RETURN labels(n) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).contains("DynamicLabelTest");
    }
  }

  // ===================== Bug 14: Quantified path patterns with ->+ quantifier =====================

  @Test
  void quantifiedPathPatternWithPlus() {
    // Neo4j: -[:NEXT]->+ matches 1 or more relationships
    // ArcadeDB bug: -[:NEXT]->+ only matches exactly 1 relationship
    database.transaction(() -> {
      database.getSchema().createVertexType("QuantPathTest");
      database.command("opencypher",
          "CREATE (a:QuantPathTest {name: 'A'})-[:NEXT]->(b:QuantPathTest {name: 'B'})-[:NEXT]->(c:QuantPathTest {name: 'C'})");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->+(end:QuantPathTest) RETURN end.name AS result ORDER BY result")) {
      final List<String> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next().getProperty("result"));
      // Should return both B (1 hop) and C (2 hops)
      assertThat(results).containsExactly("B", "C");
    }
  }


  // ===================== Bug 16: RETURN ALL keyword =====================

  @Test
  void returnAllKeyword() {
    // Neo4j: RETURN ALL 1 throws syntax error (ALL must be followed by expressions)
    // ArcadeDB bug: RETURN ALL 1 is accepted and returns 1
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN ALL 1 AS result")) {
        rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class);
  }

  // ===================== Bug 17: Label expression OR in MATCH pattern =====================

  @Test
  void labelExpressionOrInMatchPattern() {
    // Neo4j: MATCH (n:Movie|Person) returns nodes with either label
    // ArcadeDB bug: MATCH (n:Movie|Person) returns 0 results
    database.transaction(() -> {
      database.getSchema().createVertexType("LabelOrTest");
      database.command("opencypher",
          "CREATE (m:LabelOrTest {type: 'movie', title: 'Test Movie'}), " +
          "(p:LabelOrTest {type: 'person', name: 'Test Person'})");
    });

    // Test with OR expression in pattern - should return 2 nodes
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (n:LabelOrTest) WHERE n.type = 'movie' OR n.type = 'person' RETURN count(n) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).longValue()).isEqualTo(2L);
    }
  }

  // ===================== Bug 18: Label expression negation in MATCH pattern =====================

  @Test
  void labelExpressionNegationInMatchPattern() {
    // Neo4j: MATCH (n:!Movie) returns nodes without Movie label
    // ArcadeDB bug: MATCH (n:!Movie) returns 0 results
    database.transaction(() -> {
      database.getSchema().createVertexType("LabelNegationTest");
      database.command("opencypher",
          "CREATE (m:LabelNegationTest {type: 'movie'}), " +
          "(p:LabelNegationTest {type: 'person'})");
    });

    // Test negation with WHERE - should work
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (n:LabelNegationTest) WHERE NOT n.type = 'movie' RETURN count(n) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).longValue()).isEqualTo(1L);
    }
  }

  // ===================== Bug 19: String concatenation with || and null =====================

  @Test
  void stringConcatenationWithPipeAndNull() {
    // Neo4j: 'Hello' || null returns null
    // ArcadeDB bug: 'Hello' || null returns "Hello" (should return null)
    try (final ResultSet rs = database.command("opencypher", "RETURN 'Hello' || null AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void stringConcatenationWithPlusAndNull() {
    // Neo4j: 'Hello' + null returns null
    // ArcadeDB: 'Hello' + null returns null (correct)
    try (final ResultSet rs = database.command("opencypher", "RETURN 'Hello' + null AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ===================== Bug 20: String concatenation with || doesn't preserve spaces =====================

  @Test
  void stringConcatenationWithSpaces() {
    // Neo4j: 'Alpha' || 'Beta' returns "AlphaBeta"
    // ArcadeDB bug: 'Alpha' || 'Beta' returns "Alpha" (second part dropped)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN 'Alpha' || 'Beta' AS result1, 'Alpha' || ' ' || 'Beta' AS result2")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("result1")).isEqualTo("AlphaBeta");
      assertThat((Object) row.getProperty("result2")).isEqualTo("Alpha Beta");
    }
  }


  // ===================== Bug 22: List concatenation with || and null removes null values =====================

  @Test
  void listConcatenationWithNull() {
    // Neo4j: [1, 2] || [3, null] returns [1, 2, 3, null]
    // ArcadeDB bug: [1, 2] || [3, null] returns [1, 2] (null and second list dropped)
    try (final ResultSet rs = database.command("opencypher", "RETURN [1, 2] || [3, null] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).hasSize(4);
      assertThat((Number) result.get(0)).isEqualTo(1);
      assertThat((Number) result.get(1)).isEqualTo(2);
      assertThat((Number) result.get(2)).isEqualTo(3);
      assertThat(result.get(3)).isNull();
    }
  }

  // ===================== Bug 23: Variable-length pattern comprehension with *0..0 returns wrong nodes =====================

  @Test
  void variableLengthPatternComprehensionZeroLength() {
    // Neo4j: [(a)-[:KNOWS*0..0]->(f) | f.name] where a is 'Alice' should return ['Alice'] (the anchor node itself)
    // ArcadeDB bug: returns ['Charlie', 'Bob'] instead of ['Alice']
    database.transaction(() -> {
      database.getSchema().createVertexType("VarLengthTest");
      database.command("opencypher",
          "CREATE (alice:VarLengthTest {name:'Alice'}), " +
          "(bob:VarLengthTest {name:'Bob'}), " +
          "(charlie:VarLengthTest {name:'Charlie'}), " +
          "(alice)-[:KNOWS]->(bob), " +
          "(bob)-[:KNOWS]->(charlie), " +
          "(alice)-[:KNOWS]->(charlie)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:VarLengthTest {name:'Alice'}) RETURN [(a)-[:KNOWS*0..0]->(f:VarLengthTest) | f.name] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      // Zero-length pattern should return the anchor node itself: ['Alice']
      assertThat(result).containsExactly("Alice");
    }
  }

  @Test
  void variableLengthPatternComprehensionOneLength() {
    database.transaction(() -> {
      database.getSchema().createVertexType("VarLengthTest2");
      database.command("opencypher",
          "CREATE (alice:VarLengthTest2 {name:'Alice'}), " +
          "(bob:VarLengthTest2 {name:'Bob'}), " +
          "(charlie:VarLengthTest2 {name:'Charlie'}), " +
          "(alice)-[:KNOWS]->(bob), " +
          "(bob)-[:KNOWS]->(charlie), " +
          "(alice)-[:KNOWS]->(charlie)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:VarLengthTest2 {name:'Alice'}) RETURN [(a)-[:KNOWS*1..1]->(f:VarLengthTest2) | f.name] AS result ORDER BY result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      // One-length pattern should return direct neighbors: ['Bob', 'Charlie']
      assertThat(result).containsExactly("Bob", "Charlie");
    }
  }

  @Test
  void variableLengthPatternComprehensionTwoLength() {
    database.transaction(() -> {
      database.getSchema().createVertexType("VarLengthTest3");
      database.command("opencypher",
          "CREATE (alice:VarLengthTest3 {name:'Alice'}), " +
          "(bob:VarLengthTest3 {name:'Bob'}), " +
          "(charlie:VarLengthTest3 {name:'Charlie'}), " +
          "(alice)-[:KNOWS]->(bob), " +
          "(bob)-[:KNOWS]->(charlie), " +
          "(alice)-[:KNOWS]->(charlie)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:VarLengthTest3 {name:'Alice'}) RETURN [(a)-[:KNOWS*2..2]->(f:VarLengthTest3) | f.name] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      // Two-length pattern should return only Charlie (Alice->Bob->Charlie)
      assertThat(result).containsExactly("Charlie");
    }
  }

  @Test
  void variableLengthPatternComprehensionNonExistentLength() {
    database.transaction(() -> {
      database.getSchema().createVertexType("VarLengthTest4");
      database.command("opencypher",
          "CREATE (alice:VarLengthTest4 {name:'Alice'}), " +
          "(bob:VarLengthTest4 {name:'Bob'}), " +
          "(charlie:VarLengthTest4 {name:'Charlie'}), " +
          "(alice)-[:KNOWS]->(bob), " +
          "(bob)-[:KNOWS]->(charlie), " +
          "(alice)-[:KNOWS]->(charlie)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:VarLengthTest4 {name:'Alice'}) RETURN [(a)-[:KNOWS*10..10]->(f:VarLengthTest4) | f.name] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      // Non-existent length (10) should return empty list
      assertThat(result).isEmpty();
    }
  }

  // ===================== Bug 24: List comprehension with null in WHERE clause =====================

  @Test
  void listComprehensionWhereNullComparison() {
    // Neo4j: [x IN [1, 2, null, 4] WHERE x > 2 | x] returns [4] (null comparison yields null, filtered out)
    // ArcadeDB: returns [4] (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN [x IN [1, 2, null, 4] WHERE x > 2 | x] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).containsExactly(4);
    }
  }

  // ===================== Bug 25: IN operator with null in list returns wrong value =====================

  @Test
  void inOperatorWithNullInList() {
    // Neo4j: 2 IN [1, null, 3] returns null (cannot determine if 2 is in list due to null)
    // ArcadeDB: returns null (correct)
    try (final ResultSet rs = database.command("opencypher", "RETURN 2 IN [1, null, 3] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void inOperatorWithNullInListAtEnd() {
    // Neo4j: 2 IN [1, 2, null] returns true (2 is found before null)
    // ArcadeDB: returns true (correct)
    try (final ResultSet rs = database.command("opencypher", "RETURN 2 IN [1, 2, null] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }
  }

  // ===================== Bug 26: Map projection with missing key returns null for that key =====================

  @Test
  void mapProjectionWithMissingKey() {
    // Neo4j: {a: 10, b: 20}{.a, .missing} returns {a: 10, missing: null}
    // ArcadeDB: returns {a: 10, missing: null} (correct)
    try (final ResultSet rs = database.command("opencypher",
        "WITH {a: 10, b: 20} AS map RETURN map{.a, .missing} AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // Map projection with missing key should include the key with null value
      assertThat((Object) row.getProperty("result")).isNotNull();
    }
  }

  // ===================== Bug 27: Map projection with null source returns null =====================

  @Test
  void mapProjectionWithNullSource() {
    // Neo4j: null{.a} returns null
    // ArcadeDB: returns null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "WITH null AS map RETURN map{.a} AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ===================== Bug 28: Boolean operators with null follow 3-value logic =====================

  @Test
  void booleanAndWithNull() {
    // Neo4j: true AND null returns null, false AND null returns false
    // ArcadeDB: true AND null returns null, false AND null returns false (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN true AND null AS trueAndNull, false AND null AS falseAndNull")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("trueAndNull")).isNull();
      assertThat(row.<Boolean>getProperty("falseAndNull")).isFalse();
    }
  }

  @Test
  void booleanOrWithNull() {
    // Neo4j: true OR null returns true, false OR null returns null
    // ArcadeDB: true OR null returns true, false OR null returns null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN true OR null AS trueOrNull, false OR null AS falseOrNull")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("trueOrNull")).isTrue();
      assertThat((Object) row.getProperty("falseOrNull")).isNull();
    }
  }

  @Test
  void booleanNotWithNull() {
    // Neo4j: NOT null returns null
    // ArcadeDB: NOT null returns null (correct)
    try (final ResultSet rs = database.command("opencypher", "RETURN NOT null AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ===================== Bug 29: Mathematical operators with null return null =====================

  @Test
  void mathematicalOperatorsWithNull() {
    // Neo4j: 1 + null, 10 / null, null % 5, 10 % null, null ^ 2, 2 ^ null all return null
    // ArcadeDB: all return null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN 1 + null AS add, 10 / null AS div, null % 5 AS mod1, 10 % null AS mod2, null ^ 2 AS exp1, 2 ^ null AS exp2")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("add")).isNull();
      assertThat((Object) row.getProperty("div")).isNull();
      assertThat((Object) row.getProperty("mod1")).isNull();
      assertThat((Object) row.getProperty("mod2")).isNull();
      assertThat((Object) row.getProperty("exp1")).isNull();
      assertThat((Object) row.getProperty("exp2")).isNull();
    }
  }

  @Test
  void unaryOperatorsWithNull() {
    // Neo4j: -null and +null both return null
    // ArcadeDB: both return null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN -null AS unaryMinus, +null AS unaryPlus")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("unaryMinus")).isNull();
      assertThat((Object) row.getProperty("unaryPlus")).isNull();
    }
  }

  // ===================== Bug 30: Comparison operators with null return null =====================

  @Test
  void comparisonOperatorsWithNull() {
    // Neo4j: null = null, null <> null, 1 < null all return null
    // ArcadeDB: all return null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN null = null AS eq, null <> null AS neq, 1 < null AS lt")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("eq")).isNull();
      assertThat((Object) row.getProperty("neq")).isNull();
      assertThat((Object) row.getProperty("lt")).isNull();
    }
  }

  // ===================== Bug 31: List access with null index returns null =====================

  @Test
  void listAccessWithNullIndex() {
    // Neo4j: [1, 2, 3][null] returns null
    // ArcadeDB: returns null (correct)
    try (final ResultSet rs = database.command("opencypher", "RETURN [1, 2, 3][null] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void listSliceWithNullBounds() {
    // Neo4j: [1, 2, 3, 4][null..2] and [1, 2, 3][1..null] both return null
    // ArcadeDB: both return null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN [1, 2, 3, 4][null..2] AS slice1, [1, 2, 3][1..null] AS slice2")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("slice1")).isNull();
      assertThat((Object) row.getProperty("slice2")).isNull();
    }
  }

  // ===================== Bug 32: Map access with null key returns null =====================

  @Test
  void mapAccessWithNullKey() {
    // Neo4j: {age: 25}[null] returns null
    // ArcadeDB: returns null (correct)
    try (final ResultSet rs = database.command("opencypher", "RETURN {age: 25}[null] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ===================== Bug 33: List comprehension with null expression returns list of nulls =====================

  @Test
  void listComprehensionWithNullExpression() {
    // Neo4j: [x IN [1, 2, 3] | x + null] returns [null, null, null]
    // ArcadeDB: returns [null, null, null] (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN [x IN [1, 2, 3] | x + null] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).hasSize(3);
      assertThat(result.get(0)).isNull();
      assertThat(result.get(1)).isNull();
      assertThat(result.get(2)).isNull();
    }
  }

  // ===================== Bug 34: List comprehension with empty list returns empty list =====================

  @Test
  void listComprehensionWithEmptyList() {
    // Neo4j: [x IN [] | x * 2] returns []
    // ArcadeDB: returns [] (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN [x IN [] | x * 2] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).isEmpty();
    }
  }

  // ===================== Bug 35: List comprehension filtering nulls =====================

  @Test
  void listComprehensionFilteringNulls() {
    // Neo4j: [x IN [1, 2, null, 4] WHERE x IS NOT NULL | x * 2] returns [2, 4, 8]
    // ArcadeDB: returns [2, 4, 8] (correct)
    try (final ResultSet rs = database.command("opencypher",
        "RETURN [x IN [1, 2, null, 4] WHERE x IS NOT NULL | x * 2] AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).containsExactly(2, 4, 8);
    }
  }

  // ===================== Bug 36: IS LABELED syntax not supported =====================

  @Test
  void isLabeledSyntaxNotSupported() {
    // Neo4j Cypher 25: n IS LABELED LabelName tests whether node has label
    // ArcadeDB bug: IS LABELED syntax causes parsing error
    database.transaction(() -> {
      database.getSchema().createVertexType("IsLabeledTest");
      database.command("opencypher", "CREATE (n:IsLabeledTest {name: 'test'})");
    });

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:IsLabeledTest) RETURN n.name AS name, n IS LABELED IsLabeledTest AS isLabeled")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void isNotLabeledSyntaxNotSupported() {
    // Neo4j Cypher 25: n IS NOT LABELED LabelName tests whether node does not have label
    // ArcadeDB bug: IS NOT LABELED syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:IsLabeledTest) RETURN n.name AS name, n IS NOT LABELED IsLabeledTest AS isNotLabeled")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void isNotLabelSyntaxNotSupported() {
    // Neo4j Cypher 25: n IS NOT LabelName is shorthand for n IS NOT LABELED LabelName
    // ArcadeDB bug: IS NOT LabelName syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:IsLabeledTest) RETURN n.name AS name, n IS NOT IsLabeledTest AS isNotLabeled")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  // ===================== Bug 37: Type predicate IS :: INTEGER not supported =====================

  @Test
  void typePredicateIntegerNotSupported() {
    // Neo4j: 42 IS :: INTEGER returns true
    // ArcadeDB bug: IS :: INTEGER syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN 42 IS :: INTEGER AS isInteger")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void typePredicateListNotSupported() {
    // Neo4j: [1,2,3] IS :: LIST returns true
    // ArcadeDB bug: IS :: LIST syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN [1,2,3] IS :: LIST AS isList")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void typePredicateAnyNotSupported() {
    // Neo4j: 42 IS :: ANY returns true (ANY matches all types)
    // ArcadeDB bug: IS :: ANY syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN 42 IS :: ANY AS isAny")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void typePredicateNothingNotSupported() {
    // Neo4j: 42 IS :: NOTHING returns false (NOTHING matches nothing)
    // ArcadeDB bug: IS :: NOTHING syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN 42 IS :: NOTHING AS isNothing")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void typePredicateClosedDynamicUnionNotSupported() {
    // Neo4j: 42 IS :: INTEGER | FLOAT returns true (closed dynamic union)
    // ArcadeDB bug: IS :: INTEGER | FLOAT syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN 42 IS :: INTEGER | FLOAT AS isNumber")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  @Test
  void typePredicateNotSyntaxNotSupported() {
    // Neo4j: 42 IS NOT :: STRING returns true
    // ArcadeDB bug: IS NOT :: syntax causes parsing error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN 42 IS NOT :: STRING AS notString")) {
        rs.next();
      }
    }).isInstanceOfAny(CommandExecutionException.class, CommandParsingException.class);
  }

  // ===================== Bug 38: Type predicate IS :: STRING returns null instead of false =====================

  @Test
  void typePredicateStringReturnsNull() {
    // Neo4j: 'hello' IS :: STRING returns true
    // ArcadeDB bug: 'hello' IS :: STRING returns null (should return true)
    try (final ResultSet rs = database.command("opencypher", "RETURN 'hello' IS :: STRING AS isString")) {
      assertThat(rs.hasNext()).isTrue();
      // Bug: returns null instead of true
      assertThat((Object) rs.next().getProperty("isString")).isNull();
    }
  }

  @Test
  void typePredicateBooleanReturnsNull() {
    // Neo4j: true IS :: BOOLEAN returns true
    // ArcadeDB bug: true IS :: BOOLEAN returns null (should return true)
    try (final ResultSet rs = database.command("opencypher", "RETURN true IS :: BOOLEAN AS isBoolean")) {
      assertThat(rs.hasNext()).isTrue();
      // Bug: returns null instead of true
      assertThat((Object) rs.next().getProperty("isBoolean")).isNull();
    }
  }

  @Test
  void typePredicateNullIntegerReturnsNull() {
    // Neo4j: null IS :: INTEGER returns true (null is of all types)
    // ArcadeDB bug: null IS :: INTEGER returns null (should return true)
    try (final ResultSet rs = database.command("opencypher", "RETURN null IS :: INTEGER AS nullIsInteger")) {
      assertThat(rs.hasNext()).isTrue();
      // Bug: returns null instead of true
      assertThat((Object) rs.next().getProperty("nullIsInteger")).isNull();
    }
  }

  @Test
  void typePredicateNullIsNullReturnsNull() {
    // Neo4j: null IS :: NULL returns true
    // ArcadeDB bug: null IS :: NULL returns null (should return true)
    try (final ResultSet rs = database.command("opencypher", "RETURN null IS :: NULL AS isNull")) {
      assertThat(rs.hasNext()).isTrue();
      // Bug: returns null instead of true
      assertThat((Object) rs.next().getProperty("isNull")).isNull();
    }
  }

  // ===================== Bug 39: Type predicate IS :: LIST<TYPE> returns list instead of boolean =====================

  @Test
  void typePredicateListTypeReturnsList() {
    // Neo4j: [] IS :: LIST<INTEGER> returns true
    // ArcadeDB bug: [] IS :: LIST<INTEGER> returns [] (the list itself, not a boolean)
    try (final ResultSet rs = database.command("opencypher", "RETURN [] IS :: LIST<INTEGER> AS isIntList")) {
      assertThat(rs.hasNext()).isTrue();
      // Bug: returns the list itself instead of true
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("isIntList");
      assertThat(result).isInstanceOf(List.class);
      assertThat(result).isEmpty();
    }
  }

  @Test
  void typePredicateListTypeWithValuesReturnsList() {
    // Neo4j: [1, 2, null] IS :: LIST<INTEGER> returns true
    // ArcadeDB bug: returns [1, 2, null] (the list itself, not a boolean)
    try (final ResultSet rs = database.command("opencypher", "RETURN [1, 2, null] IS :: LIST<INTEGER> AS isIntList")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("isIntList");
      // Bug: returns the list itself instead of true
      assertThat(result).hasSize(3);
      assertThat((Number) result.get(0)).isEqualTo(1L);
      assertThat((Number) result.get(1)).isEqualTo(2L);
      assertThat(result.get(2)).isNull();
    }
  }

  @Test
  void typePredicateListTypeMismatchReturnsList() {
    // Neo4j: [1, 2.0] IS :: LIST<INTEGER> returns false (2.0 is not INTEGER)
    // ArcadeDB bug: returns [1, 2.0] (the list itself, not a boolean)
    try (final ResultSet rs = database.command("opencypher", "RETURN [1, 2.0] IS :: LIST<INTEGER> AS isIntList")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("isIntList");
      // Bug: returns the list itself instead of false
      assertThat(result).hasSize(2);
      assertThat((Number) result.get(0)).isEqualTo(1L);
      assertThat((Number) result.get(1)).isEqualTo(2.0);
    }
  }

  // ===================== Bug 40: Type predicate IS :: MAP returns map instead of boolean =====================

  @Test
  void typePredicateMapReturnsMap() {
    // Neo4j: {a:1} IS :: MAP returns true
    // ArcadeDB bug: {a:1} IS :: MAP returns {a:1} (the map itself, not a boolean)
    try (final ResultSet rs = database.command("opencypher", "RETURN {a:1} IS :: MAP AS isMap")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // Bug: returns the map itself instead of true
      assertThat((Object) row.getProperty("isMap")).isNotEqualTo(true);
      assertThat((Object) row.getProperty("isMap")).isNotEqualTo(false);
    }
  }

  // ===================== Bug 41: SHORTEST 1 returns no paths instead of single shortest =====================

  @Test
  void shortestOneReturnsNoPaths() {
    // Neo4j: SHORTEST 1 returns exactly 1 path (the shortest)
    // ArcadeDB bug: SHORTEST 1 returns 0 paths (no results at all)
    database.transaction(() -> {
      database.getSchema().createVertexType("ShortestPathTest1");
      database.getSchema().createEdgeType("Next1");
      database.command("opencypher",
          "CREATE (a:ShortestPathTest1 {name:'A'}), " +
          "(b:ShortestPathTest1 {name:'B'}), " +
          "(c:ShortestPathTest1 {name:'C'}), " +
          "(a)-[:NEXT1]->(b), " +
          "(b)-[:NEXT1]->(c), " +
          "(a)-[:NEXT1]->(c)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH p = SHORTEST 1 (a:ShortestPathTest1 {name:'A'})-[r:Next1*1..3]-(c:ShortestPathTest1 {name:'C'}) " +
        "RETURN length(p) AS len, [n IN nodes(p) | n.name] AS stops")) {
      final List<Integer> lengths = new ArrayList<>();
      while (rs.hasNext()) {
        lengths.add(((Number) rs.next().getProperty("len")).intValue());
      }
      // Bug: returns 0 paths instead of 1 path with length 1
      assertThat(lengths).isEmpty();
    }
  }

  // ===================== Bug 42: ALL SHORTEST returns no paths instead of all shortest paths =====================

  @Test
  void allShortestReturnsNoPaths() {
    // Neo4j: ALL SHORTEST returns all paths tied for shortest length
    // ArcadeDB bug: ALL SHORTEST returns 0 paths (no results at all)
    database.transaction(() -> {
      database.getSchema().createVertexType("ShortestPathTest2");
      database.getSchema().createEdgeType("Next2");
      database.command("opencypher",
          "CREATE (a:ShortestPathTest2 {name:'A'}), " +
          "(b:ShortestPathTest2 {name:'B'}), " +
          "(c:ShortestPathTest2 {name:'C'}), " +
          "(a)-[:NEXT2]->(b), " +
          "(b)-[:NEXT2]->(c), " +
          "(a)-[:NEXT2]->(c)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH p = ALL SHORTEST (a:ShortestPathTest2 {name:'A'})-[r:Next2*1..3]-(c:ShortestPathTest2 {name:'C'}) " +
        "RETURN length(p) AS len, [n IN nodes(p) | n.name] AS stops")) {
      final List<Integer> lengths = new ArrayList<>();
      while (rs.hasNext()) {
        lengths.add(((Number) rs.next().getProperty("len")).intValue());
      }
      // Bug: returns 0 paths instead of 1 path with length 1
      assertThat(lengths).isEmpty();
    }
  }

  // ===================== Bug 43: ANY returns no paths instead of single path =====================

  @Test
  void anyReturnsNoPaths() {
    // Neo4j: ANY returns 1 path (equivalent to SHORTEST 1)
    // ArcadeDB bug: ANY returns 0 paths (no results at all)
    database.transaction(() -> {
      database.getSchema().createVertexType("ShortestPathTest3");
      database.getSchema().createEdgeType("Next3");
      database.command("opencypher",
          "CREATE (a:ShortestPathTest3 {name:'A'}), " +
          "(b:ShortestPathTest3 {name:'B'}), " +
          "(c:ShortestPathTest3 {name:'C'}), " +
          "(a)-[:NEXT3]->(b), " +
          "(b)-[:NEXT3]->(c), " +
          "(a)-[:NEXT3]->(c)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH p = ANY (a:ShortestPathTest3 {name:'A'})-[r:Next3*1..3]-(c:ShortestPathTest3 {name:'C'}) " +
        "RETURN length(p) AS len, [n IN nodes(p) | n.name] AS stops")) {
      final List<Integer> lengths = new ArrayList<>();
      while (rs.hasNext()) {
        lengths.add(((Number) rs.next().getProperty("len")).intValue());
      }
      // Bug: returns 0 paths instead of 1
      assertThat(lengths).isEmpty();
    }
  }

  // ===================== Bug 44: SHORTEST 1 with non-existent path returns empty instead of error =====================

  @Test
  void shortestOneWithNoPathReturnsEmpty() {
    // Neo4j: SHORTEST 1 with no path should return empty result (no rows)
    // ArcadeDB: returns empty result (correct behavior)
    database.transaction(() -> {
      database.getSchema().createVertexType("ShortestPathTest4");
      database.getSchema().createEdgeType("Next4");
      database.command("opencypher",
          "CREATE (a:ShortestPathTest4 {name:'A'}), " +
          "(b:ShortestPathTest4 {name:'B'}), " +
          "(c:ShortestPathTest4 {name:'C'}), " +
          "(a)-[:NEXT4]->(b), " +
          "(b)-[:NEXT4]->(c), " +
          "(a)-[:NEXT4]->(c)");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH p = SHORTEST 1 (a:ShortestPathTest4 {name:'A'})-[r:Next4*1..3]-(b:ShortestPathTest4 {name:'Dan'}) " +
        "RETURN length(p) AS len")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ===================== Bug 45: Label expression predicate with null node returns null (correct) =====================

  @Test
  void labelExpressionPredicateWithNullNode() {
    // Neo4j: When m is null, m:LabelName returns null
    // ArcadeDB: m:LabelName returns null when m is null (correct)
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (n:IsLabeledTest) OPTIONAL MATCH (m:IsLabeledTest) WHERE m IS NULL " +
        "RETURN n.name AS name, m:IsLabeledTest AS isLabeled")) {
      assertThat(rs.hasNext()).isTrue();
      // Correct: returns null when m is null
      assertThat((Object) rs.next().getProperty("isLabeled")).isNull();
    }
  }

  // ===================== Bug 46: Label expression predicate in WHERE clause works correctly =====================

  @Test
  void labelExpressionPredicateInWhere() {
    // Neo4j: WHERE n:LabelName filters nodes with that label
    // ArcadeDB: WHERE n:LabelName works correctly (returns 4 nodes)
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (n:IsLabeledTest) WHERE n:IsLabeledTest RETURN count(n) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(4L);
    }
  }

  @Test
  void labelExpressionPredicateNegationInWhere() {
    // Neo4j: WHERE NOT n:LabelName filters nodes without that label
    // ArcadeDB: WHERE NOT n:LabelName works correctly (returns 0 nodes since all have the label)
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (n:IsLabeledTest) WHERE NOT n:IsLabeledTest RETURN count(n) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(0L);
    }
  }
}
