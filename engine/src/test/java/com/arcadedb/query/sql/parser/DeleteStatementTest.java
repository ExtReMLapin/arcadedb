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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DeleteStatementTest extends TestHelper {

  public DeleteStatementTest() {
    autoStartTx = true;
  }

  @Test
  public void deleteFromSubqueryWithWhereTest() {
    database.command("sql", "create document type Foo");
    database.command("sql", "create document type Bar");
    final MutableDocument doc1 = database.newDocument("Foo").set("k", "key1");
    final MutableDocument doc2 = database.newDocument("Foo").set("k", "key2");
    final MutableDocument doc3 = database.newDocument("Foo").set("k", "key3");

    doc1.save();
    doc2.save();
    doc3.save();

    final List<Document> list = new ArrayList<>();
    list.add(doc1);
    list.add(doc2);
    list.add(doc3);
    final MutableDocument bar = database.newDocument("Bar").set("arr", list);
    bar.save();

    database.command("sql", "delete from (select expand(arr) from Bar) where k = 'key2'");

    final ResultSet result = database.query("sql", "select from Foo");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(2);
    for (final ResultSet it = result; it.hasNext(); ) {
      final Document doc = it.next().toElement();
      assertThat(doc.getString("k")).isNotEqualTo("key2");
    }
    database.commit();
  }

  protected SqlParser getParserFor(final String string) {
    final InputStream is = new ByteArrayInputStream(string.getBytes());
    final SqlParser osql = new SqlParser(null, is);
    return osql;
  }

  protected SimpleNode checkRightSyntax(final String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(final String query, final boolean isCorrect) {
    final SqlParser osql = getParserFor(query);
    try {
      final SimpleNode result = osql.Parse();
      if (!isCorrect) {
        fail("");
      }
      return result;
    } catch (final Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail("");
      }
    }
    return null;
  }

}
