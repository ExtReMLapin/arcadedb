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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;

import java.util.concurrent.*;

public class DatabaseAsyncExecuteAlone extends DatabaseAsyncAbstractCallbackTask {
  private final OkCallback callback;

  public DatabaseAsyncExecuteAlone(final CountDownLatch semaphore, final OkCallback callback) {
    super(semaphore);
    this.callback = callback;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    callback.call();
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public String toString() {
    return "ExecuteAlone";
  }
}
