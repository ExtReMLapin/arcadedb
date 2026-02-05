package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for FOREACH clause in OpenCypher queries.
 */
class OpenCypherForeachTest {
  private Database database;
  private String databasePath;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    // Use unique database path per test method to avoid parallel execution conflicts
    databasePath = "./target/databases/testopencypher-foreach-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create schema
    database.getSchema().createVertexType("TestNode");
    database.getSchema().createVertexType("Item");
    database.getSchema().createEdgeType("HAS_ITEM");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testForeachWithCreateAndSubsequentMatch() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (root:TestNode {name: 'Root'}) " +
              "FOREACH (i IN [1, 2, 3] | " +
              "  CREATE (root)-[:HAS_ITEM]->(:Item {id: i}) " +
              ") " +
              "WITH root " +
              "MATCH (root)-[:HAS_ITEM]->(item) " +
              "RETURN count(item) AS total");

      assertThat((Object) result).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Result r = result.next();
      final Object total = r.getProperty("total");
      assertThat(total).isNotNull();
      assertThat(((Number) total).intValue()).isEqualTo(3);
    });
  }

  @Test
  void testForeachWithSimpleCreate() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (root:TestNode {name: 'Root'}) " +
              "FOREACH (i IN [1, 2, 3] | " +
              "  CREATE (:Item {id: i}) " +
              ")");
    });

    // Verify items were created
    final ResultSet verify = database.query("opencypher", "MATCH (item:Item) RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    assertThat(((Number) r.getProperty("total")).intValue()).isEqualTo(3);
  }

  @Test
  void testForeachWithExistingNode() {
    database.transaction(() -> {
      // First create the root node
      database.command("opencypher", "CREATE (root:TestNode {name: 'Root'})");

      // Then use FOREACH to create items connected to the root
      database.command("opencypher",
          "MATCH (root:TestNode {name: 'Root'}) " +
              "FOREACH (i IN [1, 2, 3] | " +
              "  CREATE (root)-[:HAS_ITEM]->(:Item {id: i}) " +
              ")");
    });

    // Verify items were created and connected
    final ResultSet verify = database.query("opencypher",
        "MATCH (root:TestNode {name: 'Root'})-[:HAS_ITEM]->(item:Item) RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    assertThat(((Number) r.getProperty("total")).intValue()).isEqualTo(3);
  }

  @Test
  void testForeachWithSet() {
    database.transaction(() -> {
      // Create nodes
      database.command("opencypher", "CREATE (:Item {id: 1}), (:Item {id: 2}), (:Item {id: 3})");

      // Use FOREACH to update properties
      database.command("opencypher",
          "MATCH (items:Item) " +
              "WITH collect(items) AS itemList " +
              "FOREACH (item IN itemList | " +
              "  SET item.processed = true " +
              ")");
    });

    // Verify properties were set
    final ResultSet verify = database.query("opencypher",
        "MATCH (item:Item) WHERE item.processed = true RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    assertThat(((Number) r.getProperty("total")).intValue()).isEqualTo(3);
  }
}
