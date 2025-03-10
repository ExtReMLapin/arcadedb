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
package com.arcadedb.utility;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.*;

public class LockContext {
  private final Lock lock = new ReentrantLock();

  protected void lock() {
    lock.lock();
  }

  protected void unlock() {
    lock.unlock();
  }

  protected RuntimeException manageExceptionInLock(final Throwable e) {
    if (e instanceof RuntimeException exception)
      throw exception;

    return new RuntimeException("Error in execution during lock", e);
  }

  public Object executeInLock(final Callable<Object> callable) {
    lock.lock();
    try {

      return callable.call();

    } catch (final Throwable e) {
      throw manageExceptionInLock(e);

    } finally {
      lock.unlock();
    }
  }
}
