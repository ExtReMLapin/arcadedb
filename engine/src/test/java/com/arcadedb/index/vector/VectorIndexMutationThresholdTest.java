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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the vector index mutation threshold bug.
 * <p>
 * Before the fix, any mutation (even 1 vector) caused a full graph rebuild on the next
 * vectorNeighbors query, ignoring the configured mutationsBeforeRebuild threshold.
 * After the fix, the graph is only rebuilt once the threshold has been reached.
 */
class VectorIndexMutationThresholdTest {
  private static final String DB_PATH   = "target/test-databases/VectorIndexMutationThresholdTest";
  private static final int    DIMS      = 32;
  private static final int    THRESHOLD = 5;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void graphIsNotRebuiltBelowThreshold() {
    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Chunk");
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", """
            CREATE INDEX ON Chunk (vector) LSM_VECTOR
            METADATA {
              "dimensions": %d,
              "similarity": "COSINE",
              "mutationsBeforeRebuild": %d
            }""".formatted(DIMS, THRESHOLD));
      });

      // Insert initial batch to build the graph
      insertVectors(db, 20, 0);

      final LSMVectorIndex index = getVectorIndex(db, "Chunk", "vector");

      // Trigger first graph build
      index.findNeighborsFromVector(randomVector(0), 5);
      final long rebuildsAfterFirstBuild = getGraphRebuildCount(index);

      // Add (THRESHOLD - 2) vectors — below the rebuild threshold
      insertVectors(db, THRESHOLD - 2, 100);
      index.findNeighborsFromVector(randomVector(1), 5);

      assertThat(getGraphRebuildCount(index))
          .as("Graph should NOT be rebuilt when mutations (" + (THRESHOLD - 2) + ") are below threshold (" + THRESHOLD + ")")
          .isEqualTo(rebuildsAfterFirstBuild);

      // Add enough vectors to reach the threshold
      insertVectors(db, 2, 200);
      index.findNeighborsFromVector(randomVector(2), 5);

      assertThat(getGraphRebuildCount(index))
          .as("Graph SHOULD be rebuilt once mutations reach threshold (" + THRESHOLD + ")")
          .isGreaterThan(rebuildsAfterFirstBuild);
    }
  }

  private void insertVectors(final Database db, final int count, final int seed) {
    db.transaction(() -> {
      for (int i = 0; i < count; i++) {
        db.newDocument("Chunk")
            .set("vector", randomVector(seed + i))
            .save();
      }
    });
  }

  private float[] randomVector(final int seed) {
    final Random rand = new Random(seed);
    final float[] v = new float[DIMS];
    for (int i = 0; i < DIMS; i++)
      v[i] = rand.nextFloat() * 2f - 1f;
    return v;
  }

  private LSMVectorIndex getVectorIndex(final Database db, final String typeName, final String propertyName) {
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName);
    return (LSMVectorIndex) type.getPolymorphicIndexByProperties(propertyName).getIndexesOnBuckets()[0];
  }

  private long getGraphRebuildCount(final LSMVectorIndex index) {
    final Map<String, Long> stats = index.getStats();
    return stats.get("graphRebuildCount");
  }
}
