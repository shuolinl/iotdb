/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.exception;

import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Objects;

public class SubscriptionException extends PipeException {

  public SubscriptionException(String message) {
    super(message);
  }

  protected SubscriptionException(String message, long timeStamp) {
    super(message, timeStamp);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SubscriptionException
        && Objects.equals(getMessage(), ((SubscriptionException) obj).getMessage())
        && Objects.equals(getTimeStamp(), ((SubscriptionException) obj).getTimeStamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getTimeStamp());
  }
}
