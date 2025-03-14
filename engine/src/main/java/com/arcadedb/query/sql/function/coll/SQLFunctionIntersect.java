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
package com.arcadedb.query.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;

import java.util.*;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the INTERSECTION of the collections received as parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionIntersect extends SQLFunctionMultiValueAbstract<Object> {
  public static final String NAME = "intersect";

  public SQLFunctionIntersect() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext ctx) {
    Object value = params[0];

    if (value == null)
      return Collections.emptySet();

    if (params.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      if (context == null) {
        // ADD ALL THE ITEMS OF THE FIRST COLLECTION
        if (value instanceof Collection collection) {
          context = collection.iterator();
        } else if (value instanceof Iterator) {
          context = value;
        } else if (value instanceof Iterable iterable) {
          context = iterable.iterator();
        } else {
          context = Arrays.asList(value).iterator();
        }
      } else {
        Iterator contextIterator = null;
        if (context instanceof Iterator iterator) {
          contextIterator = iterator;
        } else if (MultiValue.isMultiValue(context)) {
          contextIterator = MultiValue.getMultiValueIterator(context);
        }
        context = intersectWith(contextIterator, value);
      }
      return null;
    }

    // IN-LINE MODE (STATELESS)
    Iterator iterator = MultiValue.getMultiValueIterator(value, false);

    for (int i = 1; i < params.length; ++i) {
      value = params[i];

      if (value != null) {
        value = intersectWith(iterator, value);
        iterator = MultiValue.getMultiValueIterator(value, false);
      } else {
        return Collections.emptyIterator();
      }
    }

    final List result = new ArrayList();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }
    return result;
  }

  @Override
  public Object getResult() {
    return MultiValue.toSet(context);
  }

  static Collection intersectWith(final Iterator current, Object value) {
    final HashSet tempSet = new HashSet();

    if (!(value instanceof Set))
      value = MultiValue.toSet(value);

    for (final Iterator it = current; it.hasNext(); ) {
      final Object curr = it.next();

      if (value instanceof Collection collection) {
        if (collection.contains(curr))
          tempSet.add(curr);
      }
    }

    return tempSet;
  }

  public String getSyntax() {
    return "intersect(<field>*)";
  }
}
