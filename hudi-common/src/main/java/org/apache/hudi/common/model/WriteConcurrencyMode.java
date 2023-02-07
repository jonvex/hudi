/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDefault;
import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.exception.HoodieException;

import java.util.Locale;

/**
 * Different concurrency modes for write operations.
 */
@EnumDescription("Concurrency modes for write operations.")
public enum WriteConcurrencyMode {
  // Only a single writer can perform write ops
  @EnumDefault
  @EnumFieldDescription("Only one active writer to the table. Maximizes throughput.")
  SINGLE_WRITER("single_writer"),


  // Multiple writer can perform write ops with lazy conflict resolution using locks
  @EnumFieldDescription("Multiple writers can operate on the table with lazy conflict resolution using locks. "
      + "This means that only one writer will succeed if multiple write to the same file group")
  OPTIMISTIC_CONCURRENCY_CONTROL("optimistic_concurrency_control");

  private final String value;

  WriteConcurrencyMode(String value) {
    this.value = value;
  }

  /**
   * Getter for write concurrency mode.
   * @return
   */
  public String value() {
    return value;
  }

  /**
   * Convert string value to WriteConcurrencyMode.
   */
  public static WriteConcurrencyMode fromValue(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "single_writer":
        return SINGLE_WRITER;
      case "optimistic_concurrency_control":
        return OPTIMISTIC_CONCURRENCY_CONTROL;
      default:
        throw new HoodieException("Invalid value of Type.");
    }
  }

  @Override
  public String toString() {
    return value;
  }

  public boolean supportsOptimisticConcurrencyControl() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL;
  }

}
