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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionStrcmpciTest {

  private SQLFunctionStrcmpci function;

  @BeforeEach
  public void setup() {
    function = new SQLFunctionStrcmpci();
  }

  @Test
  public void testEmpty() {
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  public void testResult() {
    assertThat(function.execute(null, null, null, new String[] { "ThisIsATest", "THISISATEST" }, null)).isEqualTo(0);
  }

  @Test
  public void testQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionStrcmpci", (db) -> {
      ResultSet result = db.query("sql", "select strcmpci('ThisIsATest', 'THISISATEST') as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(0);

      result = db.query("sql", "select strcmpci(null, null) as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(0);

      result = db.query("sql", "select strcmpci('ThisIsATest', null) as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(1);

      result = db.query("sql", "select strcmpci(null, 'ThisIsATest') as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(-1);

      result = db.query("sql", "select strcmpci('ThisIsATest', 'THISISATESTO') as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(-1);

      result = db.query("sql", "select strcmpci('ThisIsATestO', 'THISISATEST') as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(+1);

      result = db.query("sql", "select strcmpci('ThisIsATestO', 'THISISATESTE') as strcmpci");
      assertThat(result.hasNext()).isTrue();
      assertThat((Integer) result.next().getProperty("strcmpci")).isEqualTo(+1);
    });
  }
}
